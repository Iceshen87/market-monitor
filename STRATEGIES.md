# Trading Strategies — Mint Claw

## Account Constraints
- ~$300 account on HyperLiquid
- Max position: $250, Max leverage: 5x, Max daily loss: $50
- All trades require Discord approval (William 👍/👎)
- VPN NOT needed — API works without geo-restriction

## Strategy 1: Leverage Flush Reversal (HIGHEST EDGE)
**Signal:** OI drops >5% + Price drops >3% simultaneously
**Thesis:** Overleveraged longs just got liquidated. Market makers absorbed the flow. Price snaps back.
**Entry:** Long the bottom wick — market or tight limit at flush low
**Stop:** 2.5% below entry | **TP:** 5-8%
**Frequency:** Rare on BTC/ETH, more common on illiquid alts
**Edge:** Pure alpha per William. The cascade creates a vacuum that fills quickly.
**From trading bible:** "OI↓, Price↓: Longs exiting. Liquidation."

## Strategy 2: Funding Rate Squeeze
**Signal:** Funding < -0.03% (8h) with significant OI
**Thesis:** Shorts are crowded and paying to stay short. Squeeze potential.
**Entry:** Long at mark or slight discount
**Stop:** 3% | **TP:** 6%
**Reverse:** Funding > 0.03% → short squeeze risk for longs
**From trading bible:** "Neg Funding <-0.03%, OI↑, CVD↑, Price dips → Shorts squeezed, longs win, rally"

## Strategy 3: OI/Volume Divergence
**Signal:** OI dropping while price rising = shorts covering
**Thesis:** Forced covering creates momentum. Ride the squeeze.
**Entry:** Long on confirmation (price already rising)
**Stop:** 3% | **TP:** 8%
**From trading bible:** "OI↓, Price↑: Shorts closing. Squeeze."

## Strategy 4: Capitulation Reversal
**Signal:** >8% drop in 24h + negative funding
**Thesis:** Panic selling exhausted. Oversold bounce.
**Entry:** Scale in: 50% now, 50% on further -3%
**Stop:** 5% | **TP:** 10%

## Strategy 5: Premium Discount
**Signal:** Perp trading >0.05% below spot
**Thesis:** Mispricing — perp should converge to spot
**Entry:** Limit at mark | **Stop:** 2% | **TP:** 3%
**Note:** Small edge, high win rate, low reward

## Strategy 6: DEGEN-Style Asymmetric (from William's prompt)
**Focus:** Low-cost entries with 2x-10x upside potential
**Filters (need ≥2):**
- Funding extreme
- OI/volume mismatch (crowding)
- Thin liquidity / microstructure vacuum
- Clear catalyst window
- Compression → expansion (range break)
**Sizing:** 5-20% of account per trade
**Venue:** HyperLiquid perps (including HIP-3 non-crypto)

## Strategy 7: Carry Trade (Passive)
**Signal:** Funding highly positive on a position we're short (or vice versa)
**Thesis:** Collect funding payments while delta-neutral or directionally aligned
**Note:** Small account makes this marginal, but free money is free money

## Key Combos (from trading bible)
| Funding | OI | CVD | Price | Signal |
|---------|-----|-----|-------|--------|
| Pos >0.03% | ↑ | ↓ | ↓ | Longs liquidated — dump |
| Pos >0.03% | ↑ | ↑ | ↑ | Crowded longs — correction risk |
| Neg <-0.03% | ↑ | ↑ | ↑ | Shorts squeezed — rally |
| Pos <0.01% | flat | ↑ | ↑ | Bulls recovering — low-risk long |
| Neg <-0.01% | flat | ↓ | ↓ | Bears persisting — low-risk short |

## Risk Management
- Risk 1-2% of account per trade ($3-6 actual risk with stops)
- With leverage, position sizes can be $50-250 notional
- Always use stops — at technical spots, not arbitrary %
- If 2 consecutive losses, pause and reassess
- Cut losers fast, trail winners

## HIP-3 Non-Crypto Opportunities
- HIP-3 perps = permissionless markets including traditional assets
- Often thinner liquidity = bigger moves = edge for small accounts
- William has had "good success" with these
- Monitor for funding/OI anomalies same as crypto perps
