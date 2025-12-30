#!/usr/bin/env python3
"""
Upload daily picks to Supabase database
Run this after generate_picks.py creates the JSON
"""

import os
import sys
import json
from datetime import date, datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
# Supabase credentials (from environment variables)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: Missing Supabase credentials")
    print("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables")
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def upload_picks_to_supabase(picks_json):
    """
    Upload picks to Supabase daily_picks table
    
    Args:
        picks_json: Dict from generate_picks_json() or path to JSON file
    """
    
    # If string path, load the JSON file
    if isinstance(picks_json, str):
        with open(picks_json, 'r') as f:
            picks_json = json.load(f)
    
    # Validate structure
    if 'locks' not in picks_json or 'lotto_tickets' not in picks_json or 'parlays' not in picks_json:
        raise ValueError("Invalid picks JSON - missing required keys (locks, lotto_tickets, parlays)")
    
    today = date.today().isoformat()
    
    # Prepare data for Supabase
    data = {
        "date": today,
        "generated_at": picks_json.get("generated_at", datetime.now().isoformat()),
        "locks": picks_json["locks"],
        "lotto_tickets": picks_json["lotto_tickets"],
        "parlays": picks_json["parlays"],
        "window_mode": picks_json.get("window_mode", "pretip"),
        "total_picks": picks_json.get("summary", {}).get("total", 0),
        "raw_data": picks_json,  # Store full JSON as backup
        "email_sent": False
    }
    
    print(f"[UPLOAD] Uploading picks for {today}...")
    print(f"[UPLOAD] Total picks: {data['total_picks']}")
    print(f"[UPLOAD]   - Locks: {len(data['locks'])}")
    print(f"[UPLOAD]   - Lotto Tickets: {len(data['lotto_tickets'])}")
    print(f"[UPLOAD]   - Parlays: {len(data['parlays'])}")
    
    try:
        # Upsert (insert or update if exists)
        result = supabase.table("daily_picks").upsert(
            data,
            on_conflict="date"  # Update if today's picks already exist
        ).execute()
        
        print(f"[UPLOAD] ✓ Success! Picks uploaded to Supabase")
        return result.data[0] if result.data else None
        
    except Exception as e:
        print(f"[UPLOAD] ✗ Error uploading to Supabase: {e}")
        raise


def get_todays_picks():
    """Retrieve today's picks from Supabase"""
    today = date.today().isoformat()
    
    try:
        result = supabase.table("daily_picks").select("*").eq("date", today).execute()
        
        if result.data:
            return result.data[0]
        else:
            print(f"[FETCH] No picks found for {today}")
            return None
            
    except Exception as e:
        print(f"[FETCH] Error fetching picks: {e}")
        return None


def get_subscribers():
    """Get all active subscribers for email sending"""
    try:
        result = supabase.table("subscribers").select("email").eq("is_active", True).execute()
        return [row["email"] for row in result.data]
    except Exception as e:
        print(f"[SUBSCRIBERS] Error fetching subscribers: {e}")
        return []


def add_subscriber(email):
    """Add a new subscriber"""
    try:
        result = supabase.table("subscribers").insert({
            "email": email,
            "verified_at": datetime.now().isoformat()  # Auto-verify for now
        }).execute()
        
        print(f"[SUBSCRIBER] ✓ Added: {email}")
        return result.data[0] if result.data else None
        
    except Exception as e:
        print(f"[SUBSCRIBER] ✗ Error adding {email}: {e}")
        return None


def mark_email_sent(pick_date=None):
    """Mark today's picks as email sent"""
    if pick_date is None:
        pick_date = date.today().isoformat()
    
    try:
        result = supabase.table("daily_picks").update({
            "email_sent": True,
            "email_sent_at": datetime.now().isoformat()
        }).eq("date", pick_date).execute()
        
        print(f"[EMAIL] ✓ Marked picks as sent for {pick_date}")
        return result.data[0] if result.data else None
        
    except Exception as e:
        print(f"[EMAIL] ✗ Error marking email sent: {e}")
        return None


def main():
    """CLI for uploading picks"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload picks to Supabase")
    parser.add_argument("--file", "-f", help="Path to picks JSON file")
    parser.add_argument("--test", action="store_true", help="Test connection and fetch today's picks")
    parser.add_argument("--subscribers", action="store_true", help="List all subscribers")
    parser.add_argument("--add-subscriber", help="Add subscriber email")
    
    args = parser.parse_args()
    
    if args.test:
        print("\n🧪 Testing Supabase connection...")
        picks = get_todays_picks()
        if picks:
            print(f"✓ Found picks for {picks['date']}")
            print(f"  Total: {picks['total_picks']}")
        else:
            print("✗ No picks found for today")
    
    elif args.subscribers:
        print("\n📧 Active Subscribers:")
        emails = get_subscribers()
        for email in emails:
            print(f"  • {email}")
        print(f"\nTotal: {len(emails)}")
    
    elif args.add_subscriber:
        add_subscriber(args.add_subscriber)
    
    elif args.file:
        upload_picks_to_supabase(args.file)
    
    else:
        # Try to load from default output
        default_file = "picks.json"
        if os.path.exists(default_file):
            print(f"Using {default_file}...")
            upload_picks_to_supabase(default_file)
        else:
            print("Usage:")
            print("  python upload_to_supabase.py --file picks.json")
            print("  python upload_to_supabase.py --test")
            print("  python upload_to_supabase.py --subscribers")


if __name__ == "__main__":
    main()