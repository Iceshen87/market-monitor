#!/usr/bin/env python3
"""
Auto Market Scanner — runs hl_monitor.py and outputs a concise summary.
Designed to be called by OpenClaw cron jobs.
Outputs plain text summary suitable for Discord messaging.
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from hl_monitor import run_scan, format_signal_discord

def main():
    result = run_scan()
    
    btc = result["btc"]
    signals = result["top_signals"]
    high_conviction = [s for s in signals if s["signal_strength"] >= 65]
    
    # Always output BTC summary
    lines = []
    lines.append(f"📊 **MARKET SCAN** — {result['total_assets_scanned']} assets")
    lines.append(f"BTC: **${btc['mark']:,.2f}** | 24h: {btc['chg_24h']:+.2f}% | Funding: {btc['funding_8h']:.4f}%")
    lines.append(f"Signals found: {result['signals_found']} total, {len(high_conviction)} high-conviction (≥65)")
    
    if high_conviction:
        lines.append("\n🎯 **HIGH CONVICTION SETUPS:**")
        for i, sig in enumerate(high_conviction[:3], 1):
            lines.append(f"\n**#{i}**")
            lines.append(format_signal_discord(sig))
            # If conviction >= 75, suggest a trade
            if sig["signal_strength"] >= 75:
                lines.append(f"⚡ *Conviction ≥75 — trade proposal ready. React 👍 to approve.*")
    else:
        lines.append("\n😴 No high-conviction setups. Staying flat.")
    
    print("\n".join(lines))

if __name__ == "__main__":
    main()
