#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone NBA Picks Generator
Extracts core logic from the GUI app to run headless and output JSON.
"""

import os
import sys
import json
from datetime import datetime

# Load configuration for data directory
try:
    import config
    # Set environment variable for nba_bettor to use
    os.environ["BETTOR_DATA_DIR"] = str(config.DATA_DIR)
except ImportError:
    print("Warning: config.py not found, using current directory")
    pass

# Import the core scanning logic from your main file
try:
    from nba_bettor import (
        scan_props,
        WINDOW_PRESETS,
        DEFAULT_MIN_BOOKS,
        DEFAULT_MIN_EV,
        DEFAULT_BANKROLL,
        DEFAULT_TOP_N,
    )
except ImportError:
    print("Error: Cannot import from nba_bettor.py")
    print("Make sure nba_bettor.py is in the same directory")
    sys.exit(1)


def generate_picks_json(
    window_mode="pretip",
    min_books=1,
    min_ev=0.0,
    bankroll=DEFAULT_BANKROLL,
    selected_markets=None,
    team_filter=None
):
    """
    Generate exactly 9 picks per day:
    - 3 LOCKS: High confidence favorites (55%+ confidence, -250 to -110 odds)
    - 3 LOTTO TICKETS: Best underdog picks (+100 to +400 odds)
    - 3 PARLAYS: 2-leg, 3-leg, and 4-leg combinations
    
    Returns:
        {
            "generated_at": ISO timestamp,
            "locks": [
                {
                    "rank": 1,
                    "confidence": 72,
                    "matchup": "BOS @ NYK",
                    "pick": "Jayson Tatum Over 27.5 Points",
                    "fd_odds": -150,
                    "true_prob": 68.5,
                    ...
                }
            ],
            "lotto_tickets": [
                {
                    "rank": 1,
                    "confidence": 48,
                    "matchup": "DET @ LAL", 
                    "pick": "Pistons ML",
                    "fd_odds": +220,
                    "true_prob": 42.3,
                    ...
                }
            ],
            "parlays": [
                {
                    "legs": 2,
                    "picks": ["Pick 1", "Pick 2"],
                    "combined_odds": +264,
                    "confidence": 45.2,
                    ...
                },
                {...},  # 3-leg
                {...}   # 4-leg
            ]
        }
    """
    
    # Default to all markets if not specified
    if selected_markets is None:
        selected_markets = [
            "player_points",
            "player_rebounds", 
            "player_assists",
            "player_threes",
            "h2h",
            "spreads",
            "totals"
        ]
    
    # Convert team_filter to set if provided as list/string
    if isinstance(team_filter, str):
        team_filter = {t.strip().upper() for t in team_filter.split(",") if t.strip()}
    elif isinstance(team_filter, list):
        team_filter = {t.upper() for t in team_filter if t}
    
    print(f"[PICKS] Generating daily picks with window_mode={window_mode}")
    print(f"[PICKS] Markets: {selected_markets}")
    print(f"[PICKS] Team filter: {team_filter or 'ALL'}")
    
    try:
        # Get ALL picks first (no top_n limit)
        rows = scan_props(
            selected_markets=selected_markets,
            min_books=min_books,
            min_ev=min_ev,
            bankroll=bankroll,
            top_n=999,  # Get everything
            window_mode=window_mode,
            max_per_game=10,
            max_per_player=10,
            team_filter=team_filter,
            progress_cb=None,
            status_cb=None
        )
        
        print(f"[PICKS] Found {len(rows)} total picks")
        
        # ========== LOCKS: High confidence favorites ==========
        locks_pool = []
        for r in rows:
            fd_odds = r.get("FD Odds", 0)
            confidence = r.get("Confidence", 0)
            
            # Must be favorite odds (-250 to -110) with high confidence (55%+)
            if -250 <= fd_odds < -110 and confidence >= 55:
                locks_pool.append(r)
        
        # Sort by confidence, prefer odds closer to -150
        locks_pool.sort(key=lambda r: (
            -r.get("Confidence", 0),  # Higher confidence first
            abs(r.get("FD Odds", 0) + 150)  # Closer to -150 is better
        ))
        
        locks = locks_pool[:3]
        print(f"[PICKS] Locks: {len(locks)}/3")
        
        # ========== LOTTO TICKETS: Best underdogs ==========
        lotto_pool = []
        for r in rows:
            fd_odds = r.get("FD Odds", 0)
            confidence = r.get("Confidence", 0)
            
            # Must be underdog odds (+100 to +400) with decent confidence (35%+)
            if 100 <= fd_odds <= 400 and confidence >= 35:
                lotto_pool.append(r)
        
        # Sort by confidence (best underdogs)
        lotto_pool.sort(key=lambda r: -r.get("Confidence", 0))
        
        lotto_tickets = lotto_pool[:3]
        print(f"[PICKS] Lotto Tickets: {len(lotto_tickets)}/3")
        
        # ========== PARLAYS: Build from remaining picks ==========
        # Use picks that aren't already locks or lottos
        used_keys = set()
        for r in (locks + lotto_tickets):
            key = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
            used_keys.add(key)
        
        parlay_pool = []
        for r in rows:
            key = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
            if key not in used_keys:
                fd_odds = r.get("FD Odds", 0)
                confidence = r.get("Confidence", 0)
                # Use picks with -200 to +150 odds and 50%+ confidence
                if -200 <= fd_odds <= 150 and confidence >= 50:
                    parlay_pool.append(r)
        
        parlay_pool.sort(key=lambda r: -r.get("Confidence", 0))
        
        parlays = _build_parlays(parlay_pool[:15])  # Use top 15 for building
        print(f"[PICKS] Parlays: {len(parlays)}/3")
        
        # ========== Format output ==========
        result = {
            "generated_at": datetime.now().isoformat(),
            "window_mode": window_mode,
            "locks": [_format_pick(r, i+1) for i, r in enumerate(locks)],
            "lotto_tickets": [_format_pick(r, i+1) for i, r in enumerate(lotto_tickets)],
            "parlays": parlays,
            "summary": {
                "locks": len(locks),
                "lotto_tickets": len(lotto_tickets),
                "parlays": len(parlays),
                "total": len(locks) + len(lotto_tickets) + len(parlays)
            }
        }
        
        return result
        
    except Exception as e:
        print(f"[ERROR] Failed to generate picks: {e}")
        import traceback
        traceback.print_exc()
        return {
            "generated_at": datetime.now().isoformat(),
            "error": str(e),
            "locks": [],
            "lotto_tickets": [],
            "parlays": [],
            "summary": {"total": 0}
        }


def _format_pick(r, rank):
    """Format a single pick for output"""
    # Build human-readable pick string
    market_key = r.get("Market Key", r["Market"])
    
    if market_key == "h2h":
        pick_str = f'{r["Player"]} ML'
    elif market_key == "spreads":
        pick_str = f'{r["Player"]} {float(r["Line"]):+g}'
    elif market_key == "totals":
        pick_str = f'{r["Side"]} {r["Line"]} (Total)'
    else:
        side_symbol = "o" if r["Side"] == "Over" else "u"
        short = {
            "player_points":"PTS",
            "player_rebounds":"REB",
            "player_assists":"AST",
            "player_threes":"3PM"
        }.get(market_key, market_key)
        pick_str = f'{r["Player"]} {side_symbol}{r["Line"]} {short}'
    
    return {
        "rank": rank,
        "pick": pick_str,
        "matchup": r["Matchup"],
        "tip_time": r["Tip (ET)"],
        "market": r["Market"],
        "player": r["Player"],
        "side": r["Side"],
        "line": float(r["Line"]),
        "fd_odds": int(r["FD Odds"]),
        "confidence": int(r["Confidence"]),
        "true_prob": float(r["True Prob %"]),
        "badge": r["Badge"],
    }


def _build_parlays(picks):
    """Build 2-leg, 3-leg, and 4-leg parlays from pick pool"""
    import itertools
    
    if len(picks) < 4:
        return []
    
    parlays = []
    
    # Helper to calculate parlay odds and confidence
    def calc_parlay(legs):
        # Calculate American odds for parlay
        total_decimal = 1.0
        total_prob = 1.0
        
        for leg in legs:
            fd_odds = leg["FD Odds"]
            # Convert to decimal
            if fd_odds >= 0:
                decimal = 1 + (fd_odds / 100)
            else:
                decimal = 1 + (100 / abs(fd_odds))
            total_decimal *= decimal
            
            # Multiply probabilities (with correlation discount)
            total_prob *= (leg["Confidence"] / 100.0)
        
        # Apply correlation discount (conservative)
        total_prob *= (0.85 ** (len(legs) - 1))
        
        # Convert decimal back to American
        if total_decimal >= 2.0:
            parlay_odds = int((total_decimal - 1) * 100)
        else:
            parlay_odds = int(-100 / (total_decimal - 1))
        
        return {
            "legs": len(legs),
            "picks": [_format_pick(leg, i+1)["pick"] for i, leg in enumerate(legs)],
            "details": [_format_pick(leg, i+1) for i, leg in enumerate(legs)],
            "combined_odds": parlay_odds,
            "confidence": int(total_prob * 100),
            "true_prob": round(total_prob * 100, 1)
        }
    
    # 2-leg parlay (highest confidence pair)
    best_2leg = None
    best_2leg_conf = 0
    for combo in itertools.combinations(picks[:8], 2):
        p = calc_parlay(combo)
        if p["confidence"] > best_2leg_conf:
            best_2leg = p
            best_2leg_conf = p["confidence"]
    
    if best_2leg:
        parlays.append(best_2leg)
    
    # 3-leg parlay (highest confidence triple)
    best_3leg = None
    best_3leg_conf = 0
    for combo in itertools.combinations(picks[:10], 3):
        p = calc_parlay(combo)
        if p["confidence"] > best_3leg_conf:
            best_3leg = p
            best_3leg_conf = p["confidence"]
    
    if best_3leg:
        parlays.append(best_3leg)
    
    # 4-leg parlay (highest confidence quad)
    best_4leg = None
    best_4leg_conf = 0
    for combo in itertools.combinations(picks[:12], 4):
        p = calc_parlay(combo)
        if p["confidence"] > best_4leg_conf:
            best_4leg = p
            best_4leg_conf = p["confidence"]
    
    if best_4leg:
        parlays.append(best_4leg)
    
    return parlays


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate daily NBA picks (3 locks, 3 lottos, 3 parlays)")
    parser.add_argument("--window", default="pretip", 
                       choices=["morning", "pretip", "plus_odds"],
                       help="Betting window preset")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--teams", help="Comma-separated team filter (e.g. 'BOS,NYK,LAL')")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    
    args = parser.parse_args()
    
    print(f"\n🎯 Generating daily picks...")
    print(f"   3 LOCKS (high confidence favorites)")
    print(f"   3 LOTTO TICKETS (best underdogs)")
    print(f"   3 PARLAYS (2-leg, 3-leg, 4-leg)\n")
    
    # Generate picks
    result = generate_picks_json(
        window_mode=args.window,
        team_filter=args.teams
    )
    
    # Format JSON
    json_str = json.dumps(result, indent=2 if args.pretty else None)
    
    # Output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(json_str)
        print(f"[PICKS] Saved to {args.output}")
    else:
        print(json_str)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"DAILY PICKS GENERATED")
    print(f"{'='*60}")
    print(f"Locks: {result['summary']['locks']}/3")
    for i, lock in enumerate(result.get('locks', []), 1):
        print(f"  #{i} ({lock['confidence']}%) {lock['pick']} @ {lock['fd_odds']:+d}")
    
    print(f"\nLotto Tickets: {result['summary']['lotto_tickets']}/3")
    for i, lotto in enumerate(result.get('lotto_tickets', []), 1):
        print(f"  #{i} ({lotto['confidence']}%) {lotto['pick']} @ {lotto['fd_odds']:+d}")
    
    print(f"\nParlays: {result['summary']['parlays']}/3")
    for parlay in result.get('parlays', []):
        print(f"  {parlay['legs']}-leg ({parlay['confidence']}% conf, {parlay['combined_odds']:+d})")
        for pick in parlay['picks']:
            print(f"    • {pick}")
    
    print(f"{'='*60}\n")
    
    # Exit with error code if incomplete
    total = result['summary'].get('total', 0)
    if result.get("error") or total < 9:
        print(f"⚠️  Warning: Only generated {total}/9 picks")
        sys.exit(1)


if __name__ == "__main__":
    main()