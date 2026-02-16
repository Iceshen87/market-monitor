#!/usr/bin/env python3
"""
Auto Market Scanner — runs hl_monitor.py and outputs signals.
Each high-conviction signal is separated by ===SIGNAL=== delimiter
so the calling agent can send them as individual Discord messages.
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from hl_monitor import run_scan, format_signal_discord

SIGNAL_DELIMITER = "===SIGNAL==="

def main():
    result = run_scan()
    
    btc = result["btc"]
    signals = result["top_signals"]
    high_conviction = [s for s in signals if s["signal_strength"] >= 65]
    
    # Summary line (always output)
    summary = (
        f"📊 **HL SCAN** — {result['total_assets_scanned']} assets | "
        f"BTC **${btc['mark']:,.2f}** ({btc['chg_24h']:+.2f}%) | "
        f"Funding: {btc['funding_8h']:.4f}% | "
        f"Signals: {result['signals_found']} total, {len(high_conviction)} high-conviction"
    )
    
    if not high_conviction:
        print(f"{summary}\n😴 No high-conviction setups. Staying flat.")
        return
    
    # Print summary first
    print(summary)
    
    # Then each signal separated by delimiter — agent should send each as its own Discord message
    for sig in high_conviction[:5]:
        print(SIGNAL_DELIMITER)
        print(format_signal_discord(sig))

if __name__ == "__main__":
    main()
