#!/usr/bin/env python3
"""
Test script for generate_picks.py
"""

import json
import sys

# Test if we can import the generator
try:
    from generate_picks import generate_picks_json
    print("✓ Successfully imported generate_picks_json")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)

# Test generating picks
print("\n" + "="*60)
print("TESTING DAILY PICKS GENERATION")
print("="*60 + "\n")

try:
    result = generate_picks_json(window_mode="pretip")
    
    print(f"✓ Generated daily picks:")
    print(f"  - Locks: {result['summary']['locks']}/3")
    print(f"  - Lotto Tickets: {result['summary']['lotto_tickets']}/3")
    print(f"  - Parlays: {result['summary']['parlays']}/3")
    print(f"  - Total: {result['summary']['total']}/9")
    
    # Show locks
    if result['locks']:
        print("\n🔒 LOCKS (High Confidence Favorites):")
        for lock in result['locks']:
            print(f"  #{lock['rank']} ({lock['confidence']}%) {lock['pick']} @ {lock['fd_odds']:+d}")
    
    # Show lotto tickets
    if result['lotto_tickets']:
        print("\n🎰 LOTTO TICKETS (Best Underdogs):")
        for lotto in result['lotto_tickets']:
            print(f"  #{lotto['rank']} ({lotto['confidence']}%) {lotto['pick']} @ {lotto['fd_odds']:+d}")
    
    # Show parlays
    if result['parlays']:
        print("\n🎲 PARLAYS:")
        for parlay in result['parlays']:
            print(f"  {parlay['legs']}-leg ({parlay['confidence']}% conf, {parlay['combined_odds']:+d})")
            for pick in parlay['picks']:
                print(f"    • {pick}")
    
    # Test JSON serialization
    json_str = json.dumps(result, indent=2)
    print(f"\n✓ JSON serialization successful ({len(json_str)} bytes)")
    
    print("\n" + "="*60)
    if result['summary']['total'] == 9:
        print("ALL TESTS PASSED ✓ - Got all 9 picks!")
    else:
        print(f"PARTIAL SUCCESS - Got {result['summary']['total']}/9 picks")
    print("="*60)
    
except Exception as e:
    print(f"\n✗ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)