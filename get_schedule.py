#!/usr/bin/env python3
"""
Find the first NBA game of the day and calculate when to run picks.
Returns the timestamp to schedule the workflow.
"""

import os
import sys
import json
from datetime import datetime, timedelta
import requests

# Load environment
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_KEY = os.getenv("ODDS_API_KEY", "ebdec188fe7e60ae6bcec321b7d091aa")
SPORT = "basketball_nba"
REGIONS = "us"
ODDS_FORMAT = "american"


def get_first_game_time(target_date=None):
    """
    Find the first NBA game of a specific date.
    
    Args:
        target_date: datetime object (default: today)
    
    Returns datetime of first tipoff, or None if no games.
    """
    if target_date is None:
        target_date = datetime.now().astimezone()
    
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    params = {
        "regions": REGIONS,
        "oddsFormat": ODDS_FORMAT,
        "markets": "h2h",
        "bookmakers": "fanduel",
        "apiKey": API_KEY
    }
    
    try:
        print(f"[SCHEDULE] Checking for games on {target_date.strftime('%Y-%m-%d')}...")
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        games = r.json()
        
        if not games:
            print(f"[SCHEDULE] No games found for {target_date.strftime('%Y-%m-%d')}")
            return None
        
        # Parse all game times and filter by target date
        game_times = []
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        for game in games:
            tip_time = game.get("commence_time")
            if tip_time:
                try:
                    dt = datetime.fromisoformat(tip_time.replace("Z", "+00:00"))
                    # Convert to local time
                    dt_local = dt.astimezone()
                    
                    # Only include games on the target date
                    if dt_local.strftime('%Y-%m-%d') == target_date_str:
                        game_times.append({
                            "time": dt_local,
                            "matchup": f"{game.get('away_team', 'TBD')} @ {game.get('home_team', 'TBD')}"
                        })
                except Exception as e:
                    print(f"[SCHEDULE] Error parsing time: {e}")
                    continue
        
        if not game_times:
            print("[SCHEDULE] No valid game times found")
            return None
        
        # Sort by time
        game_times.sort(key=lambda x: x["time"])
        
        first_game = game_times[0]
        print(f"\n[SCHEDULE] First game: {first_game['matchup']}")
        print(f"[SCHEDULE] Tip time: {first_game['time'].strftime('%I:%M %p %Z')}")
        
        return first_game["time"]
        
    except Exception as e:
        print(f"[SCHEDULE] Error fetching games: {e}")
        return None


def calculate_workflow_times(first_game_dt):
    """
    Calculate workflow times:
    1. Generate picks at 2:00 PM EST every day (fixed)
    2. Send emails 30 min before first game (dynamic)
    
    Returns dict with timestamps.
    """
    if not first_game_dt:
        return None
    
    # Fixed time: 2:00 PM EST for generating picks
    from datetime import timezone
    import pytz
    
    est = pytz.timezone('America/New_York')
    today_est = first_game_dt.astimezone(est).replace(hour=14, minute=0, second=0, microsecond=0)
    
    # Dynamic time: 30 min before first game for emails
    email_time = first_game_dt - timedelta(minutes=30)
    
    return {
        "first_game": first_game_dt.isoformat(),
        "first_game_local": first_game_dt.strftime("%I:%M %p %Z on %A, %B %d"),
        "generate_picks": today_est.isoformat(),
        "generate_picks_local": today_est.strftime("%I:%M %p %Z"),
        "send_emails": email_time.isoformat(),
        "send_emails_local": email_time.strftime("%I:%M %p %Z"),
    }


def should_run_now():
    """
    Check if we should run the workflow right now.
    Returns True if current time is within 60 minutes of scheduled time.
    (Allows hourly cron jobs to trigger the workflow)
    """
    first_game = get_first_game_time()
    if not first_game:
        print("[SCHEDULE] No games today - skipping workflow")
        return False
    
    times = calculate_workflow_times(first_game)
    if not times:
        return False
    
    now = datetime.now().astimezone()
    generate_time = datetime.fromisoformat(times["generate_picks"])
    
    # Check if we're within 60 minutes of generate time (before or after)
    time_diff_seconds = (now - generate_time).total_seconds()
    
    # Run if we're within 60 minutes BEFORE the scheduled time
    # This allows hourly cron jobs to trigger at the right time
    if -3600 <= time_diff_seconds <= 0:  # Between 60 min before and scheduled time
        print(f"\n[SCHEDULE] ✓ Time to run workflow!")
        print(f"[SCHEDULE] Current time: {now.strftime('%I:%M %p')}")
        print(f"[SCHEDULE] Scheduled for: {times['generate_picks_local']}")
        return True
    elif time_diff_seconds > 0:
        # Already passed the scheduled time
        print(f"\n[SCHEDULE] Workflow already ran (or missed)")
        print(f"[SCHEDULE] Was scheduled for: {times['generate_picks_local']}")
        return False
    else:
        # Too early
        print(f"\n[SCHEDULE] Not time yet")
        print(f"[SCHEDULE] Current time: {now.strftime('%I:%M %p')}")
        print(f"[SCHEDULE] Scheduled for: {times['generate_picks_local']}")
        minutes_until = int(abs(time_diff_seconds) / 60)
        print(f"[SCHEDULE] {minutes_until} minutes until workflow runs")
        return False


def main():
    """CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check NBA game schedule")
    parser.add_argument("--check", action="store_true", help="Check if workflow should run now")
    parser.add_argument("--times", action="store_true", help="Show scheduled times")
    parser.add_argument("--date", help="Check specific date (YYYY-MM-DD), e.g., 2025-12-31")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    # Parse target date
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").astimezone()
            print(f"\n[TESTING] Checking schedule for: {target_date.strftime('%A, %B %d, %Y')}\n")
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD (e.g., 2025-12-31)")
            sys.exit(1)
    
    if args.check:
        # Used by cron/scheduler to decide if workflow should run
        should_run = should_run_now()
        sys.exit(0 if should_run else 1)
    
    elif args.times:
        first_game = get_first_game_time(target_date)
        if not first_game:
            date_str = target_date.strftime('%Y-%m-%d') if target_date else 'today'
            print(f"No games scheduled for {date_str}")
            sys.exit(1)
        
        times = calculate_workflow_times(first_game)
        
        if args.json:
            print(json.dumps(times, indent=2))
        else:
            print("\n" + "="*60)
            print("GAME SCHEDULE")
            print("="*60)
            print(f"First Game:     {times['first_game_local']}")
            print(f"Generate Picks: {times['generate_picks_local']} (35 min before)")
            print(f"Send Emails:    {times['send_emails_local']} (30 min before)")
            print("="*60 + "\n")
    
    else:
        # Default: just show first game
        first_game = get_first_game_time(target_date)
        if first_game:
            times = calculate_workflow_times(first_game)
            print(f"\nGenerate picks at: {times['generate_picks_local']}")
            print(f"Send emails at: {times['send_emails_local']}\n")
        else:
            date_str = target_date.strftime('%Y-%m-%d') if target_date else 'today'
            print(f"\nNo games scheduled for {date_str}\n")


if __name__ == "__main__":
    main()