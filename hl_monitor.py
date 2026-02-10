#!/usr/bin/env python3
"""
HyperLiquid Market Monitor — Scans for high-signal trade setups
Strategies:
  1. Leverage flush reversal (extreme funding + OI drop + wick)
  2. Funding rate extremes (crowding → squeeze)
  3. OI/volume divergence
  4. Compression breakout detection
  5. Relative strength in weak market

Outputs JSON trade proposals to stdout or trade-requests dir.
"""

import json
import time
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

HL_API = "https://api.hyperliquid.xyz/info"
SNAPSHOT_DIR = Path(os.path.expanduser("~/projects/market-monitor/snapshots"))
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

# Risk params
MAX_POSITION_USD = 250
MAX_LEVERAGE = 5
ACCOUNT_SIZE = 300  # approximate

def hl_post(payload: dict) -> dict:
    """Post to HyperLiquid API."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())

def get_market_snapshot():
    """Get full market data."""
    data = hl_post({"type": "metaAndAssetCtxs"})
    meta = data[0]["universe"]
    ctxs = data[1]
    
    assets = []
    for m, c in zip(meta, ctxs):
        mark = float(c.get("markPx", 0))
        prev = float(c.get("prevDayPx", 0))
        oi = float(c.get("openInterest", 0))
        funding = float(c.get("funding", 0))
        vol24h = float(c.get("dayNtlVlm", 0))
        premium = float(c.get("premium", 0) or 0)
        
        chg_pct = ((mark - prev) / prev * 100) if prev else 0
        oi_usd = oi * mark
        
        assets.append({
            "name": m["name"],
            "maxLeverage": m.get("maxLeverage", 1),
            "mark": mark,
            "prevDay": prev,
            "chg24h": round(chg_pct, 2),
            "funding8h": round(funding * 100, 4),  # as percentage
            "annualizedFunding": round(funding * 100 * 3 * 365, 2),
            "oi": oi,
            "oiUsd": round(oi_usd),
            "vol24h": round(vol24h),
            "premium": round(premium * 100, 4),
        })
    
    return assets

def load_previous_snapshot():
    """Load most recent snapshot for comparison."""
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)
    if files:
        with open(files[0]) as f:
            return json.load(f)
    return None

def save_snapshot(assets):
    """Save current snapshot."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = SNAPSHOT_DIR / f"snapshot_{ts}.json"
    with open(path, "w") as f:
        json.dump(assets, f, indent=2)
    
    # Keep only last 48 snapshots (24h at 30min intervals)
    files = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)
    for old in files[48:]:
        old.unlink()
    
    return path

def scan_funding_extremes(assets, threshold=0.03):
    """
    Strategy: Extreme funding rates indicate crowding.
    - Very negative funding → shorts crowded → long squeeze potential
    - Very positive funding → longs crowded → short squeeze potential
    """
    signals = []
    
    for a in assets:
        funding = a["funding8h"]  # already in %
        oi_usd = a["oiUsd"]
        vol = a["vol24h"]
        
        # Need minimum liquidity
        if oi_usd < 500_000 or vol < 200_000:
            continue
        
        # Negative funding extreme — long opportunity
        if funding < -threshold:
            conviction = min(90, 50 + abs(funding) * 500 + (oi_usd / 10_000_000) * 10)
            # Penalize if squeeze already played out (price already moved >8% in our direction)
            if a["chg24h"] > 8:
                conviction = max(30, conviction - 30)
            signals.append({
                "strategy": "funding_squeeze_long",
                "asset": a["name"],
                "direction": "long",
                "signal_strength": round(conviction),
                "funding_8h": funding,
                "annualized": a["annualizedFunding"],
                "oi_usd": oi_usd,
                "vol_24h": vol,
                "mark": a["mark"],
                "chg_24h": a["chg24h"],
                "rationale": f"Funding deeply negative ({funding:.4f}%), shorts paying longs. "
                           f"Crowded short positioning with ${oi_usd:,.0f} OI. Squeeze potential.",
                "entry": "market or limit at current mark",
                "stop_pct": 3.0,
                "tp_pct": 6.0,
            })
        
        # Positive funding extreme — short opportunity  
        elif funding > threshold:
            conviction = min(90, 50 + abs(funding) * 500 + (oi_usd / 10_000_000) * 10)
            if a["chg24h"] < -8:
                conviction = max(30, conviction - 30)
            signals.append({
                "strategy": "funding_squeeze_short",
                "asset": a["name"],
                "direction": "short",
                "signal_strength": round(conviction),
                "funding_8h": funding,
                "annualized": a["annualizedFunding"],
                "oi_usd": oi_usd,
                "vol_24h": vol,
                "mark": a["mark"],
                "chg_24h": a["chg24h"],
                "rationale": f"Funding elevated ({funding:.4f}%), longs paying shorts. "
                           f"Crowded long positioning with ${oi_usd:,.0f} OI. Correction risk.",
                "entry": "market or limit at current mark",
                "stop_pct": 3.0,
                "tp_pct": 6.0,
            })
    
    return signals

def scan_oi_volume_divergence(assets, prev_assets):
    """
    Strategy: OI dropping while price drops = liquidation cascade ending.
    When OI drops sharply + price drops = longs just got flushed.
    Bottom wick after flush = pure alpha long entry.
    """
    if not prev_assets:
        return []
    
    prev_map = {a["name"]: a for a in prev_assets}
    signals = []
    
    for a in assets:
        name = a["name"]
        if name not in prev_map:
            continue
        
        prev = prev_map[name]
        
        # Need minimum liquidity
        if a["oiUsd"] < 1_000_000 or a["vol24h"] < 500_000:
            continue
        
        # Calculate OI change
        if prev["oiUsd"] > 0:
            oi_change_pct = ((a["oiUsd"] - prev["oiUsd"]) / prev["oiUsd"]) * 100
        else:
            continue
        
        # Leverage flush detection: OI down significantly + price down = liquidation cascade
        if oi_change_pct < -5 and a["chg24h"] < -3:
            signals.append({
                "strategy": "leverage_flush_reversal",
                "asset": name,
                "direction": "long",
                "signal_strength": min(85, 55 + abs(oi_change_pct) * 2 + abs(a["chg24h"]) * 2),
                "oi_change_pct": round(oi_change_pct, 2),
                "price_change": a["chg24h"],
                "funding_8h": a["funding8h"],
                "mark": a["mark"],
                "oi_usd": a["oiUsd"],
                "vol_24h": a["vol24h"],
                "rationale": f"OI dropped {oi_change_pct:.1f}% while price fell {a['chg24h']:.1f}%. "
                           f"Leverage flush — longs liquidated. Mean reversion setup.",
                "entry": "limit at current mark or slightly below",
                "stop_pct": 2.5,
                "tp_pct": 5.0,
            })
        
        # Reverse: OI down + price up = shorts covering (squeeze in progress)
        elif oi_change_pct < -5 and a["chg24h"] > 3:
            signals.append({
                "strategy": "short_squeeze_momentum",
                "asset": name,
                "direction": "long",
                "signal_strength": min(80, 50 + abs(oi_change_pct) * 1.5 + a["chg24h"] * 2),
                "oi_change_pct": round(oi_change_pct, 2),
                "price_change": a["chg24h"],
                "funding_8h": a["funding8h"],
                "mark": a["mark"],
                "oi_usd": a["oiUsd"],
                "vol_24h": a["vol24h"],
                "rationale": f"OI dropped {oi_change_pct:.1f}% while price rose {a['chg24h']:.1f}%. "
                           f"Shorts covering/squeezed. Momentum continuation.",
                "entry": "limit slightly above current mark",
                "stop_pct": 3.0,
                "tp_pct": 8.0,
            })
    
    return signals

def scan_large_moves(assets, move_threshold=8):
    """
    Strategy: Large 24h moves with high volume — potential continuation or reversal.
    """
    signals = []
    for a in assets:
        if a["oiUsd"] < 500_000:
            continue
        
        if abs(a["chg24h"]) >= move_threshold:
            # Large move down with negative funding = potential bottom
            if a["chg24h"] < -move_threshold and a["funding8h"] < -0.005:
                signals.append({
                    "strategy": "capitulation_reversal",
                    "asset": a["name"],
                    "direction": "long",
                    "signal_strength": min(75, 45 + abs(a["chg24h"]) + abs(a["funding8h"]) * 200),
                    "price_change": a["chg24h"],
                    "funding_8h": a["funding8h"],
                    "mark": a["mark"],
                    "oi_usd": a["oiUsd"],
                    "vol_24h": a["vol24h"],
                    "rationale": f"Down {a['chg24h']:.1f}% in 24h with negative funding ({a['funding8h']:.4f}%). "
                               f"Capitulation selling — oversold bounce candidate.",
                    "entry": "scale in: 50% now, 50% on further -3%",
                    "stop_pct": 5.0,
                    "tp_pct": 10.0,
                })
    
    return signals

def scan_premium_divergence(assets):
    """
    Strategy: Large premium/discount vs mark price indicates mispricing.
    Negative premium = perp trading below spot = buying opportunity.
    """
    signals = []
    for a in assets:
        if a["oiUsd"] < 1_000_000:
            continue
        
        premium = a["premium"]  # already in %
        
        if premium < -0.15:  # significant discount (tightened from -0.05)
            signals.append({
                "strategy": "premium_discount_long",
                "asset": a["name"],
                "direction": "long",
                "signal_strength": min(80, 40 + abs(premium) * 200),
                "premium_pct": premium,
                "funding_8h": a["funding8h"],
                "mark": a["mark"],
                "oi_usd": a["oiUsd"],
                "rationale": f"Perp trading at {premium:.4f}% discount to spot. "
                           f"Mispricing — expect convergence.",
                "entry": "limit at current mark",
                "stop_pct": 2.0,
                "tp_pct": 3.0,
            })
    
    return signals

def format_signal_discord(signal):
    """Format a signal for Discord message."""
    emoji = "🟢" if signal["direction"] == "long" else "🔴"
    return (
        f"{emoji} **{signal['strategy'].upper().replace('_', ' ')}** — {signal['asset']}\n"
        f"Direction: **{signal['direction'].upper()}** | Conviction: {signal['signal_strength']}/100\n"
        f"Mark: ${signal['mark']:,.4f} | 24h: {signal.get('chg_24h', signal.get('price_change', 'N/A'))}%\n"
        f"Funding (8h): {signal['funding_8h']:.4f}% | OI: ${signal['oi_usd']:,.0f}\n"
        f"Rationale: {signal['rationale']}\n"
        f"Entry: {signal['entry']}\n"
        f"Stop: -{signal['stop_pct']}% | TP: +{signal['tp_pct']}%\n"
    )

def format_trade_request(signal, require_approval=True):
    """Format signal as IPC trade request."""
    # Convert to trade request format
    size_usd = min(MAX_POSITION_USD, ACCOUNT_SIZE * 0.15)  # 15% of account per trade
    leverage = min(MAX_LEVERAGE, 3)  # default 3x
    
    is_buy = signal["direction"] == "long"
    side = "buy" if is_buy else "sell"
    
    return {
        "request_id": f"auto-{signal['asset']}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "symbol": f"{signal['asset']}-PERP",
        "side": side,
        "size": round(size_usd / signal["mark"], 6),
        "leverage": leverage,
        "order_type": "market",
        "requires_approval": require_approval,
        "strategy": signal["strategy"],
        "signal_strength": signal["signal_strength"],
        "rationale": signal["rationale"],
        "stop_loss_pct": signal["stop_pct"],
        "take_profit_pct": signal["tp_pct"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def run_scan():
    """Run all scanners and return sorted signals."""
    print(f"[{datetime.now().isoformat()}] Scanning HyperLiquid markets...")
    
    assets = get_market_snapshot()
    prev_assets = load_previous_snapshot()
    save_snapshot(assets)
    
    all_signals = []
    
    # Run all strategies
    all_signals.extend(scan_funding_extremes(assets, threshold=0.03))
    all_signals.extend(scan_oi_volume_divergence(assets, prev_assets))
    all_signals.extend(scan_large_moves(assets, move_threshold=8))
    all_signals.extend(scan_premium_divergence(assets))
    
    # Sort by signal strength
    all_signals.sort(key=lambda x: x["signal_strength"], reverse=True)
    
    # BTC summary
    btc = next((a for a in assets if a["name"] == "BTC"), None)
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc": {
            "mark": btc["mark"] if btc else None,
            "funding_8h": btc["funding8h"] if btc else None,
            "chg_24h": btc["chg24h"] if btc else None,
            "oi_usd": btc["oiUsd"] if btc else None,
        },
        "total_assets_scanned": len(assets),
        "signals_found": len(all_signals),
        "top_signals": all_signals[:5],  # Top 5
        "all_signals": all_signals,
    }

if __name__ == "__main__":
    result = run_scan()
    
    print(f"\n{'='*60}")
    print(f"BTC: ${result['btc']['mark']:,.2f} | Funding: {result['btc']['funding_8h']:.4f}% | 24h: {result['btc']['chg_24h']:.2f}%")
    print(f"Scanned {result['total_assets_scanned']} assets, found {result['signals_found']} signals")
    print(f"{'='*60}\n")
    
    if result["top_signals"]:
        print("TOP SIGNALS:")
        print("-" * 40)
        for i, sig in enumerate(result["top_signals"], 1):
            print(f"\n#{i}")
            print(format_signal_discord(sig))
    else:
        print("No high-conviction signals found right now.")
    
    # Save results
    out_path = Path(os.path.expanduser("~/projects/market-monitor/latest_scan.json"))
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nResults saved to {out_path}")
