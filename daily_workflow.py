#!/usr/bin/env python3
"""
MASTER DAILY WORKFLOW
Generates picks and uploads to Supabase
Now runs 35 minutes before first game (not fixed 2:30pm)
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed")
    pass

# Import our modules
try:
    from generate_picks import generate_picks_json
    from upload_to_supabase import upload_picks_to_supabase
    from get_schedule import get_first_game_time, calculate_workflow_times
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)


def run_daily_workflow():
    """
    Complete daily workflow:
    1. Check first game time
    2. Generate picks (35 min before)
    3. Upload to Supabase
    4. (Later) Send emails (30 min before)
    """
    
    print("\n" + "="*60)
    print("NBA PICKS - DAILY WORKFLOW")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}\n")
    
    # ========== STEP 0: Check Schedule ==========
    print("📅 STEP 0: Checking game schedule...")
    first_game = get_first_game_time()
    
    if not first_game:
        print("✗ No games scheduled for today - workflow cancelled")
        sys.exit(0)
    
    times = calculate_workflow_times(first_game)
    print(f"✓ First game: {times['first_game_local']}")
    print(f"✓ Picks will be generated: {times['generate_picks_local']}")
    print(f"✓ Emails will be sent: {times['send_emails_local']}")
    
    # ========== STEP 1: Generate Picks ==========
    print("\n📊 STEP 1: Generating picks...")
    try:
        picks = generate_picks_json(window_mode="pretip")
        
        total = picks['summary']['total']
        print(f"✓ Generated {total}/9 picks")
        print(f"  - Locks: {picks['summary']['locks']}/3")
        print(f"  - Lotto Tickets: {picks['summary']['lotto_tickets']}/3")
        print(f"  - Parlays: {picks['summary']['parlays']}/3")
        
        if total < 9:
            print(f"\n⚠️  Warning: Only {total}/9 picks generated")
            print("   (Some markets may not be available yet)")
        
        # Save to file for debugging
        output_file = Path("data") / f"picks_{datetime.now().strftime('%Y%m%d')}.json"
        output_file.parent.mkdir(exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(picks, f, indent=2)
        print(f"✓ Saved to: {output_file}")
        
    except Exception as e:
        print(f"✗ Error generating picks: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # ========== STEP 2: Upload to Supabase ==========
    print("\n💾 STEP 2: Uploading to Supabase...")
    try:
        result = upload_picks_to_supabase(picks)
        print(f"✓ Uploaded picks for {result['date']}")
        
    except Exception as e:
        print(f"✗ Error uploading to Supabase: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
# ========== STEP 3: Send Emails ==========
    print("\n📧 STEP 3: Email distribution...")
    
    # Check if it's time to send emails (30 min before game)
    now = datetime.now().astimezone()
    email_time = datetime.fromisoformat(times['send_emails'])
    time_until_email = (email_time - now).total_seconds() / 60
    
    if time_until_email <= 0:
        print("   ⚠️  Email time has passed - emails should have been sent")
        print(f"   Scheduled for: {times['send_emails_local']}")
    else:
        print(f"   ⏰ Emails scheduled for: {times['send_emails_local']}")
        print(f"   ({int(time_until_email)} minutes from now)")
        print("   (Email sending will be implemented in next step)")

if __name__ == "__main__":
    run_daily_workflow()