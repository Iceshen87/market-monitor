#!/usr/bin/env python3
"""
HyperLiquid Whale & Large Trade Scanner

Monitors for:
1. Large position changes (whale entries/exits)
2. Volume spikes relative to average
3. Unusual OI buildup patterns
4. Cross-asset correlation anomalies
"""

import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HL_API = "https://api.hyperliquid.xyz/info"
DATA_DIR = Path(os.path.expanduser("~/projects/market-monitor/snapshots"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


def hl_post(payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def get_market_data():
    """Get current market snapshot with meta + asset contexts."""
    data = hl_post({"type": "metaAndAssetCtxs"})
    meta = data[0]["universe"]
    ctxs = data[1]
    
    result = {}
    for m, c in zip(meta, ctxs):
        name = m["name"]
        result[name] = {
            "mark": float(c.get("markPx", 0)),
            "prevDay": float(c.get("prevDayPx", 0)),
            "oi": float(c.get("openInterest", 0)),
            "funding": float(c.get("funding", 0)),
            "vol24h": float(c.get("dayNtlVlm", 0)),
            "premium": float(c.get("premium", 0) or 0),
        }
        result[name]["oiUsd"] = result[name]["oi"] * result[name]["mark"]
        result[name]["chg24h"] = (
            ((result[name]["mark"] - result[name]["prevDay"]) / result[name]["prevDay"] * 100)
            if result[name]["prevDay"] else 0
        )
    return result


def load_history(n: int = 10) -> list[dict]:
    """Load last N snapshots for comparison."""
    files = sorted(DATA_DIR.glob("snapshot_*.json"), reverse=True)[:n]
    history = []
    for f in files:
        try:
            with open(f) as fh:
                history.append(json.load(fh))
        except Exception:
            pass
    return history


def detect_volume_spikes(current: dict, history: list[dict], threshold: float = 3.0) -> list[dict]:
    """
    Detect assets where current volume significantly exceeds historical average.
    threshold: multiplier above average to trigger alert.
    """
    alerts = []
    
    # Build average volumes from history
    vol_sums: dict[str, list[float]] = {}
    for snap in history:
        if isinstance(snap, list):
            for a in snap:
                name = a.get("name", "")
                vol_sums.setdefault(name, []).append(a.get("vol24h", 0))
        elif isinstance(snap, dict):
            for name, data in snap.items():
                if isinstance(data, dict):
                    vol_sums.setdefault(name, []).append(data.get("vol24h", 0))
    
    for name, data in current.items():
        if name not in vol_sums or not vol_sums[name]:
            continue
        
        avg_vol = sum(vol_sums[name]) / len(vol_sums[name])
        if avg_vol < 100_000:  # Skip illiquid
            continue
        
        current_vol = data["vol24h"]
        if current_vol > avg_vol * threshold:
            ratio = current_vol / avg_vol
            alerts.append({
                "type": "volume_spike",
                "asset": name,
                "current_vol": round(current_vol),
                "avg_vol": round(avg_vol),
                "ratio": round(ratio, 2),
                "mark": data["mark"],
                "chg24h": round(data["chg24h"], 2),
                "funding": round(data["funding"] * 100, 4),
                "oiUsd": round(data["oiUsd"]),
                "alert": f"🐋 {name} volume {ratio:.1f}x above average (${current_vol:,.0f} vs avg ${avg_vol:,.0f})",
            })
    
    alerts.sort(key=lambda x: x["ratio"], reverse=True)
    return alerts


def detect_oi_buildup(current: dict, history: list[dict]) -> list[dict]:
    """
    Detect assets with steadily increasing OI — whale accumulation pattern.
    Looks for monotonically increasing OI over recent snapshots.
    """
    alerts = []
    
    # Build OI timeseries from history
    oi_series: dict[str, list[float]] = {}
    for snap in history:
        if isinstance(snap, list):
            for a in snap:
                oi_series.setdefault(a.get("name", ""), []).append(a.get("oiUsd", 0))
    
    for name, series in oi_series.items():
        if name not in current or len(series) < 3:
            continue
        
        data = current[name]
        if data["oiUsd"] < 500_000:
            continue
        
        # Check if OI has been consistently rising
        series = list(reversed(series))  # chronological order
        increases = sum(1 for i in range(1, len(series)) if series[i] > series[i-1])
        
        if increases >= len(series) - 1 and len(series) >= 3:
            total_change = ((data["oiUsd"] - series[0]) / series[0] * 100) if series[0] > 0 else 0
            if total_change > 10:  # At least 10% OI buildup
                alerts.append({
                    "type": "oi_buildup",
                    "asset": name,
                    "oi_change_pct": round(total_change, 2),
                    "current_oi": round(data["oiUsd"]),
                    "snapshots_rising": increases,
                    "mark": data["mark"],
                    "chg24h": round(data["chg24h"], 2),
                    "funding": round(data["funding"] * 100, 4),
                    "alert": f"📈 {name} OI building: +{total_change:.1f}% over {len(series)} snapshots (${data['oiUsd']:,.0f})",
                })
    
    alerts.sort(key=lambda x: x["oi_change_pct"], reverse=True)
    return alerts


def detect_correlation_anomalies(current: dict) -> list[dict]:
    """
    Find assets moving opposite to BTC — potential alpha signals.
    Strong divergence from BTC correlation is notable.
    """
    alerts = []
    btc = current.get("BTC")
    if not btc:
        return alerts
    
    btc_chg = btc["chg24h"]
    
    for name, data in current.items():
        if name in ("BTC", "ETH") or data["oiUsd"] < 500_000:
            continue
        
        chg = data["chg24h"]
        
        # Strong divergence: BTC down but alt up (or vice versa)
        if btc_chg < -2 and chg > 5:
            alerts.append({
                "type": "btc_divergence_bullish",
                "asset": name,
                "asset_chg": round(chg, 2),
                "btc_chg": round(btc_chg, 2),
                "mark": data["mark"],
                "oiUsd": round(data["oiUsd"]),
                "funding": round(data["funding"] * 100, 4),
                "alert": f"🔀 {name} +{chg:.1f}% while BTC {btc_chg:.1f}% — relative strength",
            })
        elif btc_chg > 2 and chg < -5:
            alerts.append({
                "type": "btc_divergence_bearish",
                "asset": name,
                "asset_chg": round(chg, 2),
                "btc_chg": round(btc_chg, 2),
                "mark": data["mark"],
                "oiUsd": round(data["oiUsd"]),
                "funding": round(data["funding"] * 100, 4),
                "alert": f"🔀 {name} {chg:.1f}% while BTC +{btc_chg:.1f}% — relative weakness",
            })
    
    return alerts


def run_whale_scan() -> dict:
    """Run all whale/anomaly scanners."""
    current = get_market_data()
    history = load_history(10)
    
    vol_spikes = detect_volume_spikes(current, history)
    oi_buildup = detect_oi_buildup(current, history)
    corr_anomalies = detect_correlation_anomalies(current)
    
    all_alerts = vol_spikes + oi_buildup + corr_anomalies
    
    btc = current.get("BTC", {})
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc_mark": btc.get("mark"),
        "btc_chg24h": round(btc.get("chg24h", 0), 2),
        "assets_scanned": len(current),
        "alerts": {
            "volume_spikes": vol_spikes[:5],
            "oi_buildup": oi_buildup[:5],
            "correlation_anomalies": corr_anomalies[:5],
        },
        "total_alerts": len(all_alerts),
    }


if __name__ == "__main__":
    result = run_whale_scan()
    
    print(f"\n{'='*60}")
    print(f"🐋 WHALE SCANNER — {result['timestamp'][:19]}")
    print(f"BTC: ${result['btc_mark']:,.2f} ({result['btc_chg24h']:+.2f}%)")
    print(f"Scanned {result['assets_scanned']} assets, {result['total_alerts']} alerts")
    print(f"{'='*60}")
    
    for category, alerts in result["alerts"].items():
        if alerts:
            print(f"\n## {category.replace('_', ' ').title()}")
            for a in alerts:
                print(f"  {a['alert']}")
    
    if result["total_alerts"] == 0:
        print("\nNo whale activity or anomalies detected.")
    
    # Save
    out = Path(os.path.expanduser("~/projects/market-monitor/latest_whale_scan.json"))
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
