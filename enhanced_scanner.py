#!/usr/bin/env python3
"""
Enhanced Market Scanner — Adds multi-timeframe analysis and correlation detection
to the base hl_monitor.py scanner.

New features:
1. Historical snapshot comparison (1h, 4h, 12h, 24h lookback)
2. BTC correlation filter (avoid trading against BTC trend)
3. Volume profile analysis (volume spikes as confirmation)
4. Sector rotation detection (DeFi vs L1 vs Meme strength)
5. Composite scoring with multiple signal confluence
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Import base scanner
sys.path.insert(0, str(Path(__file__).parent))
from hl_monitor import (
    get_market_snapshot, load_previous_snapshot, save_snapshot,
    scan_funding_extremes, scan_oi_volume_divergence,
    scan_large_moves, scan_premium_divergence,
    calc_dynamic_sl_tp, format_signal_discord, format_trade_request,
    SNAPSHOT_DIR
)

# ── Sector Classification ──────────────────────────────────────────────────

SECTORS = {
    "L1": ["BTC", "ETH", "SOL", "AVAX", "NEAR", "SUI", "APT", "SEI", "TIA", "INJ"],
    "DeFi": ["UNI", "AAVE", "MKR", "CRV", "DYDX", "JUP", "RAY", "ORCA", "DRIFT"],
    "Meme": ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI", "MEME", "MYRO"],
    "AI": ["FET", "RNDR", "TAO", "ARKM", "WLD", "AKT", "AIOZ"],
    "Gaming": ["IMX", "GALA", "AXS", "SAND", "MANA", "PIXEL"],
    "Infrastructure": ["LINK", "GRT", "FIL", "AR", "PYTH", "W", "JTO"],
}

def classify_asset(name: str) -> str:
    """Get sector for an asset."""
    for sector, assets in SECTORS.items():
        if name in assets:
            return sector
    return "Other"


# ── Multi-Timeframe Snapshot Analysis ──────────────────────────────────────

def load_snapshot_at(hours_ago: float) -> Optional[list]:
    """Load the snapshot closest to N hours ago."""
    target_time = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    target_ts = target_time.strftime("%Y%m%d_%H%M%S")
    
    files = sorted(SNAPSHOT_DIR.glob("*.json"))
    best = None
    best_diff = float("inf")
    
    for f in files:
        # Extract timestamp from filename: snapshot_YYYYMMDD_HHMMSS.json
        parts = f.stem.split("_", 1)
        if len(parts) < 2:
            continue
        file_ts = parts[1]
        # Simple string comparison works for sortable timestamps
        diff = abs(int(file_ts.replace("_", "")) - int(target_ts.replace("_", "")))
        if diff < best_diff:
            best_diff = diff
            best = f
    
    if best:
        try:
            with open(best) as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass
    return None


def multi_timeframe_analysis(assets: list) -> dict:
    """Analyze price/OI changes across multiple timeframes."""
    timeframes = {"1h": 1, "4h": 4, "12h": 12, "24h": 24}
    
    results = {}
    for tf_name, hours in timeframes.items():
        prev = load_snapshot_at(hours)
        if not prev:
            continue
        
        prev_map = {a["name"]: a for a in prev}
        tf_data = {}
        
        for a in assets:
            if a["name"] not in prev_map:
                continue
            p = prev_map[a["name"]]
            
            price_chg = ((a["mark"] - p["mark"]) / p["mark"] * 100) if p["mark"] else 0
            oi_chg = ((a["oiUsd"] - p["oiUsd"]) / p["oiUsd"] * 100) if p["oiUsd"] else 0
            vol_ratio = (a["vol24h"] / p["vol24h"]) if p["vol24h"] else 1
            
            tf_data[a["name"]] = {
                "price_chg": round(price_chg, 2),
                "oi_chg": round(oi_chg, 2),
                "vol_ratio": round(vol_ratio, 2),
            }
        
        results[tf_name] = tf_data
    
    return results


# ── BTC Correlation Filter ─────────────────────────────────────────────────

def btc_trend_filter(assets: list, mtf: dict) -> dict:
    """
    Determine BTC trend across timeframes.
    Returns bias recommendation.
    """
    btc = next((a for a in assets if a["name"] == "BTC"), None)
    if not btc:
        return {"bias": "neutral", "confidence": 0}
    
    bullish_tf = 0
    bearish_tf = 0
    
    for tf_name, tf_data in mtf.items():
        if "BTC" in tf_data:
            chg = tf_data["BTC"]["price_chg"]
            if chg > 0.5:
                bullish_tf += 1
            elif chg < -0.5:
                bearish_tf += 1
    
    total = bullish_tf + bearish_tf
    if total == 0:
        return {"bias": "neutral", "confidence": 0, "funding": btc["funding8h"]}
    
    if bullish_tf > bearish_tf:
        conf = round((bullish_tf / max(total, 1)) * 100)
        return {"bias": "bullish", "confidence": conf, "funding": btc["funding8h"]}
    elif bearish_tf > bullish_tf:
        conf = round((bearish_tf / max(total, 1)) * 100)
        return {"bias": "bearish", "confidence": conf, "funding": btc["funding8h"]}
    
    return {"bias": "neutral", "confidence": 50, "funding": btc["funding8h"]}


# ── Sector Rotation ────────────────────────────────────────────────────────

def sector_rotation(assets: list) -> dict:
    """Analyze which sectors are outperforming/underperforming."""
    sector_perf = {}
    
    for a in assets:
        sector = classify_asset(a["name"])
        if sector not in sector_perf:
            sector_perf[sector] = {"total_chg": 0, "count": 0, "assets": []}
        sector_perf[sector]["total_chg"] += a["chg24h"]
        sector_perf[sector]["count"] += 1
        sector_perf[sector]["assets"].append({"name": a["name"], "chg": a["chg24h"]})
    
    for sector in sector_perf:
        count = sector_perf[sector]["count"]
        sector_perf[sector]["avg_chg"] = round(sector_perf[sector]["total_chg"] / count, 2) if count else 0
        sector_perf[sector]["assets"].sort(key=lambda x: x["chg"], reverse=True)
    
    # Rank sectors
    ranked = sorted(sector_perf.items(), key=lambda x: x[1]["avg_chg"], reverse=True)
    
    return {
        "sectors": {k: v for k, v in ranked},
        "strongest": ranked[0][0] if ranked else None,
        "weakest": ranked[-1][0] if ranked else None,
    }


# ── Volume Spike Detection ─────────────────────────────────────────────────

def detect_volume_spikes(assets: list, mtf: dict, threshold: float = 2.0) -> list:
    """Detect assets with unusual volume (>2x normal)."""
    spikes = []
    
    for tf_name, tf_data in mtf.items():
        for name, data in tf_data.items():
            if data["vol_ratio"] >= threshold:
                asset = next((a for a in assets if a["name"] == name), None)
                if asset and asset["oiUsd"] > 500_000:
                    spikes.append({
                        "asset": name,
                        "timeframe": tf_name,
                        "vol_ratio": data["vol_ratio"],
                        "price_chg": data["price_chg"],
                        "sector": classify_asset(name),
                    })
    
    spikes.sort(key=lambda x: x["vol_ratio"], reverse=True)
    return spikes[:10]


# ── Composite Signal Scoring ───────────────────────────────────────────────

def apply_confluence_scoring(signals: list, btc_bias: dict, mtf: dict, vol_spikes: list) -> list:
    """
    Adjust signal conviction based on confluence factors:
    - BTC alignment: +10 if trade aligns with BTC trend, -15 if against
    - Volume spike: +10 if asset has unusual volume
    - Multi-timeframe alignment: +5 per aligned timeframe
    """
    vol_spike_assets = {s["asset"] for s in vol_spikes}
    
    for sig in signals:
        adjustments = []
        
        # BTC alignment
        if btc_bias["bias"] != "neutral":
            if (btc_bias["bias"] == "bullish" and sig["direction"] == "long") or \
               (btc_bias["bias"] == "bearish" and sig["direction"] == "short"):
                sig["signal_strength"] = min(95, sig["signal_strength"] + 10)
                adjustments.append(f"BTC {btc_bias['bias']} alignment (+10)")
            else:
                sig["signal_strength"] = max(20, sig["signal_strength"] - 15)
                adjustments.append(f"Against BTC {btc_bias['bias']} trend (-15)")
        
        # Volume spike confirmation
        if sig["asset"] in vol_spike_assets:
            sig["signal_strength"] = min(95, sig["signal_strength"] + 10)
            adjustments.append("Volume spike confirmation (+10)")
        
        # Multi-timeframe alignment
        aligned_tfs = 0
        for tf_name, tf_data in mtf.items():
            if sig["asset"] in tf_data:
                chg = tf_data[sig["asset"]]["price_chg"]
                if (sig["direction"] == "long" and chg > 0) or \
                   (sig["direction"] == "short" and chg < 0):
                    aligned_tfs += 1
        
        if aligned_tfs >= 2:
            bonus = aligned_tfs * 5
            sig["signal_strength"] = min(95, sig["signal_strength"] + bonus)
            adjustments.append(f"MTF alignment x{aligned_tfs} (+{bonus})")
        
        sig["confluence"] = adjustments
        sig["sector"] = classify_asset(sig["asset"])
    
    signals.sort(key=lambda x: x["signal_strength"], reverse=True)
    return signals


# ── Main Enhanced Scanner ──────────────────────────────────────────────────

def run_enhanced_scan() -> dict:
    """Run enhanced scan with all new features."""
    print(f"[{datetime.now().isoformat()}] Enhanced market scan starting...")
    
    # Get current data
    assets = get_market_snapshot()
    prev_assets = load_previous_snapshot()
    save_snapshot(assets)
    
    # Multi-timeframe analysis
    mtf = multi_timeframe_analysis(assets)
    
    # BTC trend
    btc_bias = btc_trend_filter(assets, mtf)
    
    # Sector rotation
    sectors = sector_rotation(assets)
    
    # Volume spikes
    vol_spikes = detect_volume_spikes(assets, mtf)
    
    # Run base signals
    all_signals = []
    all_signals.extend(scan_funding_extremes(assets, threshold=0.03))
    all_signals.extend(scan_oi_volume_divergence(assets, prev_assets))
    all_signals.extend(scan_large_moves(assets, move_threshold=8))
    all_signals.extend(scan_premium_divergence(assets))
    
    # Deduplicate
    seen = {}
    for sig in all_signals:
        key = sig["asset"]
        if key not in seen or sig["signal_strength"] > seen[key]["signal_strength"]:
            seen[key] = sig
    all_signals = list(seen.values())
    
    # Apply confluence scoring
    all_signals = apply_confluence_scoring(all_signals, btc_bias, mtf, vol_spikes)
    
    # BTC summary
    btc = next((a for a in assets if a["name"] == "BTC"), None)
    
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc": {
            "mark": btc["mark"] if btc else None,
            "funding_8h": btc["funding8h"] if btc else None,
            "chg_24h": btc["chg24h"] if btc else None,
            "oi_usd": btc["oiUsd"] if btc else None,
            "trend_bias": btc_bias,
        },
        "sector_rotation": sectors,
        "volume_spikes": vol_spikes,
        "total_assets_scanned": len(assets),
        "signals_found": len(all_signals),
        "top_signals": all_signals[:5],
        "all_signals": all_signals,
    }
    
    return result


def format_enhanced_report(result: dict) -> str:
    """Format a comprehensive market report."""
    lines = []
    btc = result["btc"]
    
    lines.append(f"📊 **Enhanced Market Scan** — {result['timestamp'][:16]}")
    lines.append(f"BTC: ${btc['mark']:,.2f} | 24h: {btc['chg_24h']:.2f}% | Funding: {btc['funding_8h']:.4f}%")
    lines.append(f"BTC Bias: **{btc['trend_bias']['bias'].upper()}** ({btc['trend_bias']['confidence']}% conf)")
    lines.append("")
    
    # Sector rotation
    sr = result["sector_rotation"]
    if sr.get("strongest") and sr.get("weakest"):
        lines.append(f"🔄 Sectors: **{sr['strongest']}** leading | **{sr['weakest']}** lagging")
        for sector, data in list(sr["sectors"].items())[:4]:
            lines.append(f"  {sector}: {data['avg_chg']:+.2f}% avg ({data['count']} assets)")
        lines.append("")
    
    # Volume spikes
    if result["volume_spikes"]:
        lines.append("📈 Volume Spikes:")
        for vs in result["volume_spikes"][:5]:
            lines.append(f"  {vs['asset']} ({vs['sector']}): {vs['vol_ratio']:.1f}x volume [{vs['timeframe']}]")
        lines.append("")
    
    # Signals
    lines.append(f"Scanned {result['total_assets_scanned']} assets, found {result['signals_found']} signals")
    lines.append("=" * 50)
    
    if result["top_signals"]:
        for i, sig in enumerate(result["top_signals"], 1):
            lines.append(f"\n#{i}")
            lines.append(format_signal_discord(sig))
            if sig.get("confluence"):
                lines.append(f"  Confluence: {', '.join(sig['confluence'])}")
    else:
        lines.append("\nNo high-conviction signals found.")
    
    return "\n".join(lines)


if __name__ == "__main__":
    result = run_enhanced_scan()
    report = format_enhanced_report(result)
    print(report)
    
    # Save
    out_path = Path(os.path.expanduser("~/projects/market-monitor/latest_enhanced_scan.json"))
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved to {out_path}")
