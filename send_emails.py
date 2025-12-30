#!/usr/bin/env python3
"""
Send daily picks to email subscribers via Resend.com
"""

import os
import sys
import json
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import resend
    from upload_to_supabase import supabase, get_todays_picks, get_subscribers, mark_email_sent
except ImportError as e:
    print(f"Error: Missing dependencies. Run: pip install resend")
    print(f"Details: {e}")
    sys.exit(1)


# Resend API key
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if not RESEND_API_KEY:
    print("ERROR: RESEND_API_KEY not set")
    sys.exit(1)

resend.api_key = RESEND_API_KEY


def create_email_html(picks_data):
    """Generate HTML email from picks data"""
    
    locks = picks_data.get('locks', [])
    lottos = picks_data.get('lotto_tickets', [])
    parlays = picks_data.get('parlays', [])
    date = picks_data.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: monospace; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
            .container {{ max-width: 600px; margin: 0 auto; background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 30px; }}
            h1 {{ color: #3b82f6; text-transform: uppercase; margin-top: 0; }}
            h2 {{ color: #60a5fa; border-bottom: 2px solid #1e40af; padding-bottom: 10px; margin-top: 30px; }}
            .pick {{ background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 15px; margin: 10px 0; }}
            .pick-header {{ display: flex; justify-content: space-between; margin-bottom: 10px; }}
            .confidence {{ background: #1e40af; color: white; padding: 4px 12px; border-radius: 4px; font-weight: bold; }}
            .pick-title {{ font-size: 18px; color: white; font-weight: bold; margin: 8px 0; }}
            .pick-details {{ color: #94a3b8; font-size: 14px; }}
            .odds {{ color: #3b82f6; font-weight: bold; font-size: 16px; }}
            .parlay {{ background: #1e293b; border: 1px solid #3b82f6; border-radius: 6px; padding: 15px; margin: 10px 0; }}
            .parlay-leg {{ padding: 8px; margin: 5px 0; background: #0f172a; border-radius: 4px; color: #e2e8f0; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #334155; color: #64748b; font-size: 12px; text-align: center; }}
            .empty {{ color: #64748b; font-style: italic; padding: 20px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🏀 NAO'S PICKS - {date}</h1>
            
            <h2>🔒 LOCKS</h2>
    """
    
    if locks:
        for lock in locks:
            html += f"""
            <div class="pick">
                <div class="pick-header">
                    <span class="confidence">{lock['confidence']}% CONFIDENCE</span>
                    <span class="odds">{lock['fd_odds']:+d}</span>
                </div>
                <div class="pick-title">{lock['pick']}</div>
                <div class="pick-details">{lock['matchup']} • {lock['tip_time']}</div>
            </div>
            """
    else:
        html += '<div class="empty">No locks available today</div>'
    
    html += '<h2>🎰 LOTTO TICKETS</h2>'
    
    if lottos:
        for lotto in lottos:
            html += f"""
            <div class="pick">
                <div class="pick-header">
                    <span class="confidence">{lotto['confidence']}% CONFIDENCE</span>
                    <span class="odds">{lotto['fd_odds']:+d}</span>
                </div>
                <div class="pick-title">{lotto['pick']}</div>
                <div class="pick-details">{lotto['matchup']} • {lotto['tip_time']}</div>
            </div>
            """
    else:
        html += '<div class="empty">No lotto tickets available today</div>'
    
    html += '<h2>🎲 PARLAYS</h2>'
    
    if parlays:
        for parlay in parlays:
            html += f"""
            <div class="parlay">
                <div class="pick-header">
                    <strong>{parlay['legs']}-LEG PARLAY</strong>
                    <span class="odds">{parlay['combined_odds']:+d} ({parlay['confidence']}%)</span>
                </div>
            """
            for i, pick in enumerate(parlay['picks'], 1):
                html += f'<div class="parlay-leg">LEG {i}: {pick}</div>'
            html += '</div>'
    else:
        html += '<div class="empty">No parlays available today</div>'
    
    html += """
            <div class="footer">
                <p>Visit <a href="https://nba-picks-site.vercel.app" style="color: #3b82f6;">nba-picks-site.vercel.app</a> for more details</p>
                <p>You're receiving this because you subscribed to NBA Picks</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def send_picks_email():
    """Send today's picks to all subscribers"""
    
    print("\n" + "="*60)
    print("SENDING DAILY PICKS EMAILS")
    print("="*60 + "\n")
    
    # Get today's picks
    print("📊 Fetching today's picks from Supabase...")
    picks_data = get_todays_picks()
    
    if not picks_data:
        print("✗ No picks found for today - skipping email")
        return False
    
    print(f"✓ Found picks for {picks_data['date']}")
    print(f"  - Locks: {len(picks_data.get('locks', []))}")
    print(f"  - Lotto Tickets: {len(picks_data.get('lotto_tickets', []))}")
    print(f"  - Parlays: {len(picks_data.get('parlays', []))}")
    
    # Get subscribers
    print("\n📧 Fetching subscribers...")
    subscribers = get_subscribers()
    
    if not subscribers:
        print("✗ No subscribers found")
        return False
    
    print(f"✓ Found {len(subscribers)} subscribers")
    
    # Generate email HTML
    print("\n✉️  Generating email...")
    email_html = create_email_html(picks_data)
    
    # Send emails
    print(f"\n📤 Sending emails to {len(subscribers)} subscribers...")
    
    success_count = 0
    fail_count = 0
    
    for email in subscribers:
        try:
            params = {
                "from": "NBA Picks <picks@naobettor.com>",
                "to": [email],
                "subject": f"🏀 Today's NBA Picks - {picks_data['date']}",
                "html": email_html,
            }
            
            resend.Emails.send(params)
            print(f"  ✓ Sent to {email}")
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ Failed to send to {email}: {e}")
            fail_count += 1
    
    # Mark as sent in database
    if success_count > 0:
        print("\n💾 Marking emails as sent in database...")
        mark_email_sent()
        print("✓ Database updated")
    
    # Summary
    print("\n" + "="*60)
    print("EMAIL SENDING COMPLETE")
    print("="*60)
    print(f"✓ Successfully sent: {success_count}/{len(subscribers)}")
    if fail_count > 0:
        print(f"✗ Failed: {fail_count}/{len(subscribers)}")
    print("="*60 + "\n")
    
    return success_count > 0


if __name__ == "__main__":
    success = send_picks_email()
    sys.exit(0 if success else 1)