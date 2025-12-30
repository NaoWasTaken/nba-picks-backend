#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FanDuel +EV Finder — ULTIMATE one-file build (props + ML + Spread + Totals)
Target: Beat FanDuel using non-FD market consensus + conservative model blend.

What's in here:
- Trimmed/weighted no-vig consensus from other books (FD excluded)
- Usage model (BallDontLie) blended & clamped to market envelope (player props)
- Team alias normalizer (cross-API canonicalization)
- Team-state injury pressure for ML/Spread (shown as +X / +Y (ΔZ))
- Confidence = PURE hit probability (independent of CLV/steam/etc.)
- Steam tracking, line advantage, EV, Kelly sizing (fractional, capped)
- Portfolio caps, CSV export
- Local SQLite logging for tick (steam) + "would bet" rows
- Threaded event fetch + retries; robust type checks
- **Window presets**: Morning (soft) vs Pre-tip (confirmed) → different sorting & light tuning
- **EV display safety-recalc** so UI can't show a wrong EV if anything drifted
- **Badge thresholds are configurable** (HIGH/MED/LOW cutoffs)

CHANGELOG (non-invariant):
- 2025-11-12 (LOGIC-FIXES): Fixed model blend direction, Kelly correlation penalties, confidence scoring,
  sign-filter removal, team injury scaling, parlay correlation, window EV requirements, steam window tuning.
  Bumped APP_VERSION to mark mathematical changes.
"""

APP_VERSION = "1.1.0"  # bumped due to math/logic fixes

import os, sys, csv, time, threading, queue, statistics, math, json, sqlite3, pathlib, traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import itertools
from io import StringIO

if TYPE_CHECKING:
    from tkinter import BooleanVar as TkBooleanVar
    from tkinter import StringVar as TkStringVar
    from tkinter.ttk import Treeview as TtkTreeview

# ========================= Your keys (env overrides) =========================
EMBEDDED_ODDS_API_KEY       = os.getenv("ODDS_API_KEY", "ebdec188fe7e60ae6bcec321b7d091aa").strip()
EMBEDDED_SPORTSDATAIO_KEY   = os.getenv("SPORTSDATAIO_API_KEY", "64b545542bc54b868243d54b4649c78d").strip()
EMBEDDED_BALLDONTLIE_KEY    = os.getenv("BALLDONTLIE_API_KEY", "482bdc53-f88f-4c10-a644-4a2794d6dc19").strip()

# =============================== Dependencies ===============================
try:
    import requests
except Exception:
    print("This app needs 'requests'. Install it with:\n  pip install requests")
    sys.exit(1)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SESSION = requests.Session()
SESSION.headers.update({"Accept-Encoding": "gzip, deflate", "User-Agent": "FD-EV-Finder/1.1"})
adapter = HTTPAdapter(
    pool_connections=50,
    pool_maxsize=50,
    max_retries=Retry(total=2, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)

use_bootstrap = True
try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
    from ttkbootstrap.dialogs import Messagebox
    tk = tb.tk
    ttk = tb.ttk
except Exception:
    import tkinter as tk
    from tkinter import ttk, messagebox
    use_bootstrap = False

try:
    import tkinter.font as tkfont
except Exception:
    tkfont = None

try:
    from PIL import Image, ImageTk, ImageDraw
    HAS_PIL = True
except Exception:
    HAS_PIL = False

# ==== Brand palette ====
BRAND_DARK      = "#1f1f1f"
BRAND_BG        = "#0a0002"
BRAND_RED_0     = "#120004"
BRAND_RED_1     = "#3a0710"
BRAND_RED_2     = "#6b0b18"
BRAND_RED_3     = "#9e0f22"
BRAND_RED_4     = "#eb102e"
BRAND_TEXT      = "#ffffff"
BRAND_TEXT_DIM  = "#e8e8e8"

def apply_brand_styles(root):
    """Create on-brand ttk/ttkbootstrap styles for buttons and progressbar."""
    try:
        style = tb.Style() if use_bootstrap else ttk.Style()
    except Exception:
        return

    style.configure(
        "Brand.TButton",
        foreground=BRAND_TEXT,
        background=BRAND_RED_2,
        bordercolor=BRAND_RED_3,
        focusthickness=1,
        focuscolor=BRAND_RED_3,
        padding=(10, 6)
    )
    style.map(
        "Brand.TButton",
        background=[("active", BRAND_RED_3), ("pressed", BRAND_RED_1), ("disabled", "#333333")],
        foreground=[("disabled", "#aaaaaa")],
        bordercolor=[("active", BRAND_RED_4), ("pressed", BRAND_RED_2)]
    )

    style.configure(
        "BrandTool.TButton",
        foreground=BRAND_TEXT_DIM,
        background=BRAND_RED_1,
        bordercolor=BRAND_RED_2,
        padding=(8, 4)
    )
    style.map(
        "BrandTool.TButton",
        background=[("active", BRAND_RED_2), ("pressed", BRAND_RED_0)],
        bordercolor=[("active", BRAND_RED_3), ("pressed", BRAND_RED_2)],
        foreground=[("active", BRAND_TEXT), ("disabled", "#888888")]
    )

    style.configure(
        "Brand.Horizontal.TProgressbar",
        troughcolor=BRAND_BG,
        background=BRAND_RED_3,
        darkcolor=BRAND_RED_2,
        lightcolor=BRAND_RED_4,
        bordercolor=BRAND_RED_2
    )

# ============================== Config / Constants ==========================
API_KEY     = EMBEDDED_ODDS_API_KEY
SPORT       = "basketball_nba"
REGIONS     = "us"
ODDS_FORMAT = "american"

FANDUEL_KEY = "fanduel"
OTHER_BOOKS = ["draftkings","betmgm","caesars","pointsbetus","betrivers","espnbet","wynnbet"]
SHARP_BOOKS = {"draftkings","betmgm","caesars"}

BRAND_FONT_PATH = os.path.join("assets", "punk.ttf")

BORDERLESS = True
START_FULLSCREEN = True

ALL_PROP_MARKETS = [
    ("Points",  "player_points"),
    ("Rebounds","player_rebounds"),
    ("Assists", "player_assists"),
    ("3PM",     "player_threes"),
]

TEAM_MARKETS = [
    ("Moneyline", "h2h"),
    ("Spread",    "spreads"),
    ("Totals",    "totals"),
]

ALL_MARKETS = ALL_PROP_MARKETS + TEAM_MARKETS

DEFAULT_MIN_BOOKS   = 3
DEFAULT_MIN_EV      = 2.0
DEFAULT_BANKROLL    = 1000.0
DEFAULT_TOP_N       = 10
KELLY_CAP_PCT       = 2.5

BADGE_THRESHOLDS = {"HIGH": 70, "MED": 60, "LOW": 55}

APP_DIR = pathlib.Path(".")
WEIGHTS_PATH = APP_DIR / "weights.json"
DB_PATH = APP_DIR / "market_ticks.sqlite"

DEFAULT_WEIGHTS = {
    "fanduel": 1.0, "draftkings": 1.0, "betmgm": 1.0, "caesars": 1.0,
    "pointsbetus": 1.0, "betrivers": 1.0, "espnbet": 1.0, "wynnbet": 1.0,
}

CURRENT_KELLY_MULT = 0.5

# ============================== Betting Window Presets ======================
# FIXED: Morning now requires min EV and raised min_true_prob to avoid -EV bets
WINDOW_PRESETS = {
    "morning": {
        "label": "Morning (soft)",
        "sort": "EV",
        "min_books_delta": -1,
        "trim": 0.20,
        "ml_bump_scale": 0.5,
        "spread_bump_scale": 0.4,
        "require_ev": True,
        "require_gap": True,
        "min_gap_cents": 5,
        "min_avg_gap_cents": 5,
        "min_true_prob_pct": 52,
        "steam_window_sec": 14400,
    },
    "pretip": {
        "label": "Pre-tip (high-confidence)",  # ✅ RENAMED
        "sort": "CONF",
        "min_books_delta": -2,
        "trim": 0.15,
        "ml_bump_scale": 1.0,
        "spread_bump_scale": 0.6,
        "require_ev": False,  # ✅ CHANGED: Don't require +EV
        "require_gap": False,  # ✅ CHANGED: Don't require price gaps
        "min_gap_cents": 0,
        "min_avg_gap_cents": 0,
        "min_true_prob_pct": 55,  # ✅ CHANGED: 55%+ = more likely to hit
        "steam_window_sec": 1800,
        # ✅ NEW PARAMETERS:
        "fd_odds_min": -240,  # Only show -180 or better (e.g., -150, -120, +110)
        "fd_odds_max": -140,  # Only show -120 or worse (e.g., -150, -180)
        "mode": "confidence",  # Flag to indicate confidence-based filtering
    },
    "plus_odds": {  # NEW
        "label": "Plus Odds Hunter",
        "sort": "EV",
        "min_books_delta": 0,
        "trim": 0.18,
        "ml_bump_scale": 0.7,
        "spread_bump_scale": 0.5,
        "require_ev": True,
        "require_gap": True,
        "min_gap_cents": 8,  # Bigger gap required for plus odds
        "min_avg_gap_cents": 6,
        "min_true_prob_pct": 45,  # Lower threshold (underdogs)
        "steam_window_sec": 7200,  # 2 hours
        "fd_odds_min": 100,  # ✅ Only plus odds
        "fd_odds_max": 400,  # ✅ Cap at +400 (avoid lottery tickets)
        "mode": "plus_odds",
    },
}

# ============================== Team alias map ==============================
TEAM_ALIASES: Dict[str, str] = {
    "atlanta hawks":"ATL","hawks":"ATL","atl":"ATL",
    "boston celtics":"BOS","celtics":"BOS","bos":"BOS",
    "brooklyn nets":"BKN","nets":"BKN","bkn":"BKN","brooklyn":"BKN",
    "charlotte hornets":"CHA","hornets":"CHA","cha":"CHA","charlotte":"CHA",
    "chicago bulls":"CHI","bulls":"CHI","chi":"CHI",
    "cleveland cavaliers":"CLE","cavaliers":"CLE","cavs":"CLE","cle":"CLE",
    "detroit pistons":"DET","pistons":"DET","det":"DET",
    "indiana pacers":"IND","pacers":"IND","ind":"IND",
    "miami heat":"MIA","heat":"MIA","mia":"MIA",
    "milwaukee bucks":"MIL","bucks":"MIL","mil":"MIL",
    "new york knicks":"NYK","knicks":"NYK","nyk":"NYK","new york":"NYK",
    "orlando magic":"ORL","magic":"ORL","orl":"ORL",
    "philadelphia 76ers":"PHI","76ers":"PHI","sixers":"PHI","phi":"PHI","philadelphia":"PHI",
    "toronto raptors":"TOR","raptors":"TOR","tor":"TOR",
    "washington wizards":"WAS","wizards":"WAS","was":"WAS","washington":"WAS",
    "dallas mavericks":"DAL","mavericks":"DAL","mavs":"DAL","dal":"DAL",
    "denver nuggets":"DEN","nuggets":"DEN","den":"DEN",
    "golden state warriors":"GSW","warriors":"GSW","gsw":"GSW","golden state":"GSW",
    "houston rockets":"HOU","rockets":"HOU","hou":"HOU",
    "los angeles clippers":"LAC","la clippers":"LAC","clippers":"LAC","lac":"LAC",
    "los angeles lakers":"LAL","la lakers":"LAL","lakers":"LAL","lal":"LAL",
    "memphis grizzlies":"MEM","grizzlies":"MEM","mem":"MEM",
    "minnesota timberwolves":"MIN","timberwolves":"MIN","wolves":"MIN","min":"MIN",
    "new orleans pelicans":"NOP","pelicans":"NOP","pels":"NOP","nop":"NOP","new orleans":"NOP",
    "oklahoma city thunder":"OKC","thunder":"OKC","okc":"OKC",
    "phoenix suns":"PHX","suns":"PHX","phx":"PHX",
    "portland trail blazers":"POR","trail blazers":"POR","blazers":"POR","por":"POR",
    "sacramento kings":"SAC","kings":"SAC","sac":"SAC",
    "san antonio spurs":"SAS","spurs":"SAS","sas":"SAS",
    "utah jazz":"UTA","jazz":"UTA","uta":"UTA",
}

def team_key(name: str) -> str:
    s = (name or "").strip().lower()
    s = " ".join(s.replace(".", "").replace("-", " ").split())
    return TEAM_ALIASES.get(s, s.upper())

# ============================== Odds helpers ================================
def american_to_implied_prob(a: int) -> float:
    try:
        a = int(a)
    except Exception:
        return 0.5
    return 100.0/(a+100.0) if a >= 100 else abs(a)/(abs(a)+100.0)

def implied_prob_to_american(p: float) -> int:
    p = max(1e-6, min(1-1e-6, float(p)))
    return int(round(-100 * p / (1 - p))) if p >= 0.5 else int(round(100 * (1 - p) / p))

def american_to_decimal(a: int) -> float:
    try:
        a = int(a)
    except Exception:
        return 1.0
    return 1 + (a/100.0) if a >= 100 else 1 + (100.0/abs(a))

def price_better_for_bettor(fd: int, other: int) -> bool:
    if fd >= 0 and other >= 0: return fd > other
    if fd <= 0 and other <= 0: return abs(fd) < abs(other)
    return fd > other

def cents_diff(fd: int, other: Optional[int]) -> int:
    if other is None: return 0
    if fd >= 0 and other >= 0: return int(fd) - int(other)
    if fd <= 0 and other <= 0: return abs(int(other)) - abs(int(fd))
    return (fd if fd > 0 else 0) + (abs(other) if other < 0 else 0)

def fmt_time_short(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z","+00:00")).astimezone()
        s = dt.strftime("%I:%M %p")
        return s.lstrip("0") if s.startswith("0") else s
    except Exception:
        return iso_str

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

# ============================ Parlay helpers ====================
def _row_true_prob(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("True Prob %", 0.0)) / 100.0
    except Exception:
        return 0.0

def _row_dec_odds(row: Dict[str, Any]) -> float:
    try:
        return american_to_decimal(int(row.get("FD Odds", 0)))
    except Exception:
        return 1.0

# FIXED: Parlay correlation discount now more aggressive
def _parlay_independence_discount(legs: List[Dict[str, Any]]) -> float:
    """
    FIXED: More aggressive correlation penalty.
    Old formula: 1 - min(0.25, pts/50) with floor 0.70
    New formula: 1 - min(0.40, pts/30) with floor 0.50
    """
    keys_rows = []
    for r in legs:
        keys_rows.append({
            "Matchup": r["Matchup"],
            "Player":  r["Player"],
            "Market":  r["Market"],
            "Side":    r["Side"],
            "Line":    r["Line"],
        })
    pen_map, _flag_map = correlation_penalty(keys_rows)
    total_pts = 0
    for r in keys_rows:
        k = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
        total_pts += pen_map.get(k, 0)
    disc = math.exp(-total_pts / 20.0)
    return max(0.30, disc)

def _parlay_metrics(legs: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    if not legs:
        return 0.0, 1.0, -100.0
    p = 1.0
    dec = 1.0
    for r in legs:
        p *= _row_true_prob(r)
        dec *= _row_dec_odds(r)
    p *= _parlay_independence_discount(legs)
    ev = p * dec - 1.0
    return p, dec, round(ev * 100.0, 2)

# =============================== Local storage ==============================
def load_book_weights():
    try:
        with open(WEIGHTS_PATH, "r", encoding="utf-8") as f:
            w = json.load(f)
        if not isinstance(w, dict):
            raise ValueError("weights.json not a dict")
        for k, v in DEFAULT_WEIGHTS.items():
            if k not in w or not isinstance(w[k], (int, float)):
                w[k] = v
        return w
    except Exception:
        try:
            with open(WEIGHTS_PATH, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_WEIGHTS, f, indent=2)
        except Exception:
            pass
        return DEFAULT_WEIGHTS.copy()

def save_book_weights(weights: dict):
    with open(WEIGHTS_PATH, "w", encoding="utf-8") as f:
        json.dump(weights, f, indent=2)

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("""CREATE TABLE IF NOT EXISTS ticks(
        ts INTEGER, event_id TEXT, matchup TEXT, tip_et TEXT,
        player TEXT, market TEXT, line REAL, side TEXT,
        book TEXT, price INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS bets(
        ts INTEGER, event_id TEXT, matchup TEXT, tip_et TEXT,
        player TEXT, market TEXT, line REAL, side TEXT,
        fd_price INTEGER, fair_prob REAL, true_prob REAL,
        confidence INTEGER, badge TEXT
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_ticks_event ON ticks(event_id, market, player, line, side, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_ticks_ts ON ticks(ts)")
    con.commit(); con.close()

BOOK_WEIGHTS = load_book_weights()
db_init()

def _db_execmany(sql: str, rows: List[tuple], tries: int = 4, sleep_s: float = 0.08):
    if not rows:
        return
    last_err = None
    for t in range(tries):
        con = None
        try:
            con = sqlite3.connect(DB_PATH, timeout=2.0)
            cur = con.cursor()
            cur.executemany(sql, rows)
            con.commit()
            return
        except sqlite3.OperationalError as e:
            last_err = e
            time.sleep(sleep_s * (1.6 ** t))
        except Exception as e:
            last_err = e
            break
        finally:
            try:
                if con: con.close()
            except Exception:
                pass
    if last_err:
        print(f"[db] write failed after retries: {last_err}")

# =============================== HTTP helpers ===============================
def http_get_json(url: str, params: Dict[str, Any] | None = None,
                  headers: Dict[str, str] | None = None, timeout: int = 20) -> Any:
    try:
        print(f"[HTTP] GET {url} params={params}")  # Debug
        r = SESSION.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
        print(f"[HTTP] Status: {r.status_code}")  # Debug
        r.raise_for_status()
        data = r.json()
        print(f"[HTTP] Response keys: {data.keys() if isinstance(data, dict) else type(data)}")  # Debug
        return data
    except requests.exceptions.HTTPError as e:
        print(f"[HTTP ERROR] {e} - Response: {getattr(e.response, 'text', 'N/A')}")
        return None
    except Exception as e:
        print(f"[HTTP EXCEPTION] {type(e).__name__}: {e}")
        return None

def fetch_featured_events():
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    params = {"regions": REGIONS, "oddsFormat": ODDS_FORMAT, "markets": "h2h",
              "bookmakers": FANDUEL_KEY, "apiKey": API_KEY}
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json(), r.headers
    except requests.HTTPError as e:
        return None, {"error": f"HTTP error: {e}", "url": getattr(r,'url',url)}
    except Exception as e:
        return None, {"error": f"Request error: {e}"}

def fetch_event_props_retry(event_id: str, markets: List[str],
                            tries: int = 3, base_timeout: int = 8, backoff: float = 1.5):
    last_err = None
    for t in range(tries):
        timeout = int(base_timeout * (backoff ** t))
        try:
            url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds"
            params = {
                "regions": REGIONS,
                "oddsFormat": ODDS_FORMAT,
                "bookmakers": ",".join([FANDUEL_KEY] + OTHER_BOOKS),
                "markets": ",".join(markets),
                "apiKey": API_KEY,
            }
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json(), r.headers
        except requests.HTTPError as e:
            last_err = {"error": f"HTTP error: {e}", "url": getattr(r, "url", url)}
            time.sleep(0.8)
        except Exception as e:
            last_err = {"error": f"Request error: {e}"}
            time.sleep(0.5)
    return None, last_err or {"error": "Unknown error"}

# ================== Injury (RapidAPI) & Minutes (BallDontLie) ================
BALLDONTLIE_KEY = EMBEDDED_BALLDONTLIE_KEY

# AI-CHANGE (injury source):
#   - Was: free NBA.com JSON (https://ak-static.cms.nba.com/referee/injury/Injury-Report.json)
#   - Now: RapidAPI endpoint (configured via RAPIDAPI_NBA_INJURY_URL / HOST / KEY)
#   - Caching, normalization, and confidence logic are preserved so callers behave the same.

# --- RapidAPI injury config (fill these in with your actual values) ---
RAPIDAPI_NBA_INJURY_BASE_URL  = "https://nba-injuries-reports.p.rapidapi.com/injuries/nba"  # <- TODO
RAPIDAPI_NBA_INJURY_HOST = "nba-injuries-reports.p.rapidapi.com"                           # <- TODO
RAPIDAPI_NBA_KEY         = "ab2682f02amshe6ec1c1998aabcdp12d4c2jsn21b87d76eef1"                        # <- TODO

_injuries_cache: list | None = None
_injuries_fetched_at = 0.0
_inj_index_cache: Dict[str, dict] = {}
_inj_index_built_at = 0.0
_inj_index_for_ts = 0.0

_name_id_cache: Dict[str, int] = {}
_minutes_cache: Dict[int, tuple[float, float, int]] = {}

def _norm(s: str) -> str:
    return " ".join((s or "").lower().replace(".", "").replace("-", " ").split())

def fetch_nba_official_injuries() -> list:
    """
    Fetch injury report from RapidAPI NBA endpoint (paid/free tier via RAPIDAPI_NBA_KEY).

    Returns:
      A list of "rows" with at least:
        {
          "Name": full_name,
          "Player": full_name,
          "Team": team_name,
          "TeamAbbr": team_abbr,
          "InjuryStatus": status,
          "Status": status,
          "Injury": injury,
          "Comment": comment,
          "Updated": iso_timestamp
        }
    """
    global _injuries_cache, _injuries_fetched_at, _inj_index_cache, _inj_index_built_at, _inj_index_for_ts

    # Cache for 5 minutes
    now_ts = time.time()
    if _injuries_cache is not None and (now_ts - _injuries_fetched_at) < 300:
        return _injuries_cache


    try:
        headers = {
            "x-rapidapi-key": RAPIDAPI_NBA_KEY,
            "x-rapidapi-host": RAPIDAPI_NBA_INJURY_HOST,
        }

        # This API wants: /injuries/nba/YYYY-MM-DD
        from datetime import timezone, timedelta
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        url = f"{RAPIDAPI_NBA_INJURY_BASE_URL}/{date_str}"

        r = SESSION.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        print(f"[Injury API] SUCCESS: Got {len(data) if isinstance(data, list) else 'dict'} items from {date_str}")
        if isinstance(data, list) and len(data) > 0:
            print(f"[Injury API] Sample: {data[0]}")  # Print first injury
        elif isinstance(data, dict):
            print(f"[Injury API] Dict keys: {data.keys()}")

        rows: list[dict] = []

        # This specific API returns a simple list of records:
        # [{"date": "...", "team": "...", "player": "...", "status": "...", "reason": "...", "reportTime": "..."}]
        if isinstance(data, list):
            payload = data
        elif isinstance(data, dict):
            # Just in case it ever wraps under something like "response"
            payload = data.get("response") or data.get("results") or []
        else:
            payload = []

        for item in payload:
            if not isinstance(item, dict):
                continue

            # This API uses 'player' as a single full-name string
            full_name = (item.get("player") or "").strip()
            team_name = (item.get("team") or "").strip()

            # We don't actually get a standard abbreviation field here, so leave blank.
            team_abbr = ""

            status = (item.get("status") or "").strip()
            # 'reason' is the injury text
            injury = (item.get("reason") or "").strip()
            # Use reportTime as a lightweight "comment" (11AM / 3PM / 5PM etc.)
            comment = (item.get("reportTime") or "").strip()

            if not full_name or not status:
                continue

            row = {
                "Name": full_name,
                "Player": full_name,
                "Team": team_name,
                "TeamAbbr": team_abbr,
                "InjuryStatus": status,
                "Status": status,
                "Injury": injury,
                "Comment": comment,
                "Updated": datetime.now().isoformat(),
            }
            rows.append(row)

        now = time.time()
        _injuries_cache = rows
        _injuries_fetched_at = now
        _inj_index_cache = {_norm(r["Name"]): r for r in rows}
        _inj_index_built_at = now
        _inj_index_for_ts = now

        return rows

    except Exception as e:
        print(f"[Injury API] Failed to fetch RapidAPI NBA injuries: {e}")
        # Return cached data if available
        return _injuries_cache or []

def _normalize_injury_status(raw: str | None) -> str:
    """Normalize RapidAPI / NBA injury status to standard format."""
    s = _norm(raw or "")
    if not s:
        return ""

    if "out" in s:
        return "Out"
    if "doubt" in s:
        return "Doubtful"
    if "question" in s or "gtd" in s:
        return "Q/GTD"
    if "probable" in s:
        return "Probable"
    if "available" in s:
        return ""

    if "rest" in s or "load" in s or "management" in s:
        return "Q/GTD"

    return raw if raw else ""

def _injury_row_for_name(player_name: str) -> Optional[dict]:
    """Find injury row for player name with fuzzy matching."""
    injuries = fetch_nba_official_injuries()
    if not injuries:
        return None

    nm = _norm(player_name)

    # Check cache first
    global _inj_index_cache, _inj_index_for_ts, _injuries_fetched_at
    if _inj_index_cache and _inj_index_for_ts == _injuries_fetched_at:
        if nm in _inj_index_cache:
            return _inj_index_cache[nm]

    # Fuzzy match if not in cache
    best, best_score = None, 0
    tgt = set(nm.split())

    for r in injuries:
        rname = _norm(r.get("Name", ""))
        if not rname:
            continue

        score = len(tgt & set(rname.split()))
        if score > best_score:
            best, best_score = r, score

    return best if best_score > 0 else None

def injury_status_for_name(player_name: str) -> str | None:
    """Get normalized injury status for player."""
    row = _injury_row_for_name(player_name)
    if not row:
        return None

    raw = row.get("InjuryStatus") or row.get("Status") or ""
    return _normalize_injury_status(raw)

def injury_confidence_adjust(player_name: str, is_opponent: bool = False) -> tuple[float, str]:
    """Calculate confidence adjustment based on injury status."""
    try:
        status = injury_status_for_name(player_name)
        if status is None:
            return 0.0, ""

        s = status
        if not is_opponent:
            # For the player we're betting on (props)
            if s == "Out":      return -50.0, "Out"  # Much larger penalty
            if s == "Doubtful": return -30.0, "Doubtful"
            if s == "Q/GTD":    return -15.0, "Q/GTD"
            if s == "Probable": return -5.0, "Probable"
        else:
            # For opponents (affects team totals/spreads)
            if s == "Out":      return +5.0, f"Opp-{s}"
            if s == "Doubtful": return +3.0, f"Opp-{s}"
            if s == "Q/GTD":    return +2.0, f"Opp-{s}"
            if s == "Probable": return 0.0, f"Opp-{s}"
        return 0.0, s
    except Exception:
        return 0.0, ""

def bdl_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {EMBEDDED_BALLDONTLIE_KEY}"} if EMBEDDED_BALLDONTLIE_KEY else {}

def bdl_find_player_id(player_name: str) -> int | None:
    key = _norm(player_name)
    if key in _name_id_cache:
        print(f"[BDL CACHE HIT] {player_name} -> {_name_id_cache[key]}")
        return _name_id_cache[key]

    # Split name into parts
    parts = player_name.strip().split()
    if not parts:
        return None
    
    # Try first_name filter with the first word
    first_name = parts[0]
    
    url = "https://api.balldontlie.io/v1/players"
    print(f"[BDL] Searching for player: {player_name}")
    
    # Use first_name filter instead of broken 'search'
    data = http_get_json(url, params={"first_name": first_name, "per_page": 100}, headers=bdl_headers())

    time.sleep(1)

    if not data or not isinstance(data, dict): 
        print(f"[BDL ERROR] Invalid response type: {type(data)}")
        return None
    
    candidates = data.get("data", [])
    print(f"[BDL] Found {len(candidates)} candidates for first_name='{first_name}'")
    
    if not candidates:
        print(f"[BDL] No candidates found")
        return None
    
    # Now do fuzzy matching on the full name
    tgt = set(key.split())
    print(f"[BDL] Target tokens: {tgt}")
    
    best, best_score = None, 0
    for c in candidates:
        nm = _norm(f"{c.get('first_name','')} {c.get('last_name','')}")
        score = len(tgt & set(nm.split()))
        if score > best_score:
            best, best_score = c, score
    
    if best and best_score > 0:
        pid = best.get("id")
        full_name = f"{best.get('first_name','')} {best.get('last_name','')}"
        print(f"[BDL] Matched '{player_name}' to '{full_name}' (ID={pid}, score={best_score})")
        if pid:
            _name_id_cache[key] = pid
        return pid
    
    print(f"[BDL] No match found for '{player_name}'")
    return None

def parse_min_to_float(min_str: str | None) -> float:
    if not min_str: return 0.0
    if ":" in min_str:
        try:
            mm, ss = min_str.split(":")
            return float(int(mm) + int(ss)/60.0)
        except Exception:
            return 0.0
    try:
        return float(min_str)
    except Exception:
        return 0.0

_minutes_cache: Dict[int, tuple[float, float, int]] = {}
_minutes_cache_expiry: Dict[int, float] = {}  # ✅ ADD THIS

def bdl_recent_minutes(pid: int, last_n: int = 7) -> tuple[float,float,int]:
    """
    Fetch recent minutes using SportsData.io PlayerGameStatsByDate.
    Filters out injury-shortened games (<15 min) to get true playing time baseline.
    Cache expires after 2 hours.
    OPTIMIZED: Smart date range checking (stops early when enough games found).
    """
    # DELETE THIS LATER - API CURRENTLY DISABLED
    return (0.0, 0.0, 0)  # ⬅️ ADD THIS LINE (disables API calls)
    now = time.time()

    global _minutes_cache_expiry

    # ✅ Check cache with expiry
    if pid in _minutes_cache:
        cache_age = now - _minutes_cache_expiry.get(pid, 0)
        if cache_age < 7200:  # 2 hours
            print(f"[MINUTES CACHE HIT] PID {pid} -> {_minutes_cache[pid]} (age: {cache_age/60:.1f}min)")
            return _minutes_cache[pid]
        else:
            print(f"[MINUTES CACHE EXPIRED] PID {pid} (age: {cache_age/60:.1f}min)")
    
    print(f"[MINUTES] Fetching stats via SportsData.io for BDL PID {pid}")
    
    # Get player name from our cache
    player_name = None
    for cached_name, cached_id in _name_id_cache.items():
        if cached_id == pid:
            player_name = cached_name
            break
    
    if not player_name:
        print(f"[MINUTES] No cached name for PID {pid}")
        _minutes_cache[pid] = (0.0, 0.0, 0)
        _minutes_cache_expiry[pid] = now
        return _minutes_cache[pid]
    
    try:
        api_key = EMBEDDED_SPORTSDATAIO_KEY
        
        # Step 1: Find player on SportsData.io
        url = f"https://api.sportsdata.io/v3/nba/scores/json/Players"
        params = {'key': api_key}
        
        print(f"[MINUTES] Looking up '{player_name}' on SportsData.io...")
        r = SESSION.get(url, params=params, timeout=10)
        r.raise_for_status()
        players = r.json()
        
        # Match by name
        target_tokens = set(_norm(player_name).split())
        sports_data_player_id = None
        matched_name = None
        
        for p in players:
            full_name = f"{p.get('FirstName','')} {p.get('LastName','')}"
            name_norm = _norm(full_name)
            name_tokens = set(name_norm.split())
            
            if len(target_tokens & name_tokens) >= len(target_tokens):
                sports_data_player_id = p.get('PlayerID')
                matched_name = full_name
                break
        
        if not sports_data_player_id:
            print(f"[MINUTES] Player not found on SportsData.io")
            _minutes_cache[pid] = (0.0, 0.0, 0)
            _minutes_cache_expiry[pid] = now
            return _minutes_cache[pid]
        
        print(f"[MINUTES] Matched to '{matched_name}' (ID: {sports_data_player_id})")
        
        # Step 2: Fetch games from recent dates (OPTIMIZED: smart date range)
        from datetime import datetime, timedelta
        
        all_player_games = []
        today = datetime.now()
        
        # ✅ OPTIMIZED: Start with 30 days, extend to 60 only if needed
        initial_days = 30
        max_days = 60
        
        # Phase 1: Check last 30 days
        for days_ago in range(initial_days):
            date = today - timedelta(days=days_ago)
            date_str = date.strftime('%Y-%m-%d')
            
            try:
                url = f"https://api.sportsdata.io/v3/nba/stats/json/PlayerGameStatsByDate/{date_str}"
                params = {'key': api_key}
                
                r = SESSION.get(url, params=params, timeout=10)
                if r.status_code != 200:
                    continue
                
                daily_stats = r.json()
                
                # Filter for our player
                player_stats = [g for g in daily_stats if g.get('PlayerID') == sports_data_player_id]
                
                if player_stats:
                    all_player_games.extend(player_stats)
                    print(f"[MINUTES] Found game on {date_str}")
                
                # ✅ EARLY EXIT: Stop if we have 2× the games we need (buffer for filtering)
                if len(all_player_games) >= last_n * 2:
                    print(f"[MINUTES] Early exit: found {len(all_player_games)} games in {days_ago+1} days")
                    break
                
                # Small delay to avoid rate limits
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[MINUTES] Error fetching {date_str}: {e}")
                continue
        
        # ✅ Phase 2: If still not enough games, extend to 60 days (rare - injured players)
        if len(all_player_games) < last_n * 2:
            print(f"[MINUTES] Extending search to 60 days (found only {len(all_player_games)} games in 30 days)")
            
            for days_ago in range(initial_days, max_days):
                date = today - timedelta(days=days_ago)
                date_str = date.strftime('%Y-%m-%d')
                
                try:
                    url = f"https://api.sportsdata.io/v3/nba/stats/json/PlayerGameStatsByDate/{date_str}"
                    params = {'key': api_key}
                    
                    r = SESSION.get(url, params=params, timeout=10)
                    if r.status_code != 200:
                        continue
                    
                    daily_stats = r.json()
                    player_stats = [g for g in daily_stats if g.get('PlayerID') == sports_data_player_id]
                    
                    if player_stats:
                        all_player_games.extend(player_stats)
                        print(f"[MINUTES] Found game on {date_str}")
                    
                    # Stop if we now have enough
                    if len(all_player_games) >= last_n * 2:
                        print(f"[MINUTES] Found enough games: {len(all_player_games)} total")
                        break
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"[MINUTES] Error fetching {date_str}: {e}")
                    continue
        
        if not all_player_games:
            print(f"[MINUTES] No games found in last {max_days} days")
            _minutes_cache[pid] = (0.0, 0.0, 0)
            _minutes_cache_expiry[pid] = now
            return _minutes_cache[pid]
        
        # Sort by date (most recent first)
        all_player_games.sort(key=lambda x: x.get('Day', ''), reverse=True)
        
        # Extract minutes - ONLY count games with 15+ minutes (filter out injury/DNP)
        mins = []
        injury_games = 0
        
        for game in all_player_games:
            m = float(game.get('Minutes', 0) or 0)
            if m >= 15:  # Only meaningful playing time
                mins.append(m)
            elif m > 0:  # Count very low minutes as injury flag
                injury_games += 1
            
            if len(mins) >= last_n:
                break
        
        print(f"[MINUTES DEBUG] Healthy games (15+ min): {mins}")
        print(f"[MINUTES DEBUG] Injury-shortened games: {injury_games}")
        
        if not mins:
            print(f"[MINUTES] No healthy games found (possible injury)")
            _minutes_cache[pid] = (0.0, 0.0, 0)
            _minutes_cache_expiry[pid] = now
            return _minutes_cache[pid]
        
        # Calculate median and IQR
        med = statistics.median(mins)
        if len(mins) >= 4:
            qs = sorted(mins)
            q = statistics.quantiles(qs, n=4)
            q1, q3 = q[0], q[2]
            iqr = max(0.0, q3 - q1)
        else:
            iqr = max(0.0, (max(mins) - min(mins)) * 0.5)
        
        # Red flag: if player has recent injury games, increase IQR (uncertainty)
        if injury_games > 0:
            # More moderate penalty: 20% per injury game, max 60% boost
            iqr_boost = min(0.6, injury_games * 0.2)
            iqr = iqr * (1 + iqr_boost)
            print(f"[MINUTES] Applied injury uncertainty penalty ({iqr_boost:.0%} boost)")
                
        print(f"[MINUTES] Success: med={med:.1f}, iqr={iqr:.1f}, n={len(mins)} (excluded {injury_games} injury games)")
        
        # ✅ Cache with timestamp
        _minutes_cache[pid] = (med, iqr, len(mins))
        _minutes_cache_expiry[pid] = now
        
        return _minutes_cache[pid]
        
    except Exception as e:
        print(f"[MINUTES ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        _minutes_cache[pid] = (0.0, 0.0, 0)
        _minutes_cache_expiry[pid] = now
        return _minutes_cache[pid]

def minutes_confidence_adjust(player_name: str) -> tuple[float, str]:
    print(f"\n{'='*60}")
    print(f"[MINUTES ENTRY] Called for: {player_name}")
    print(f"{'='*60}")
    try:
        pid = bdl_find_player_id(player_name)
        print(f"[MINUTES] {player_name} -> PID: {pid}")
        if not pid:
            print(f"[MINUTES] No PID found, returning 0.0")
            return 0.0, ""
        
        med, iqr, n = bdl_recent_minutes(pid, last_n=7)
        print(f"[MINUTES] Result: med={med}, iqr={iqr}, n={n}")
        
        tag = f"{med:.1f}/{iqr:.1f}(n={n})" if n > 0 else ""  # ✅ Show sample size in tag
        
        # ✅ CRITICAL FIX: Require minimum 5 games for any adjustment
        if n < 5:
            print(f"[MINUTES] Insufficient data (n={n} < 5), returning -5.0 penalty")
            return -5.0, tag  # Hard penalty: not enough data

        # ✅ NEW: For marginal samples (n=5-6), use more conservative thresholds
        elif n < 7:
            print(f"[MINUTES] Marginal data (n={n}), using conservative thresholds")
            if med >= 32 and iqr <= 3:  return +2.0, tag  # Stricter: was +3.0
            if med >= 28 and iqr <= 4:  return +1.0, tag  # Stricter: was +2.0
            if med < 18 or iqr >= 10:   return -4.0, tag  # Harsher: was -3.0
            return -1.0, tag  # Default: slight penalty for uncertainty

        # ✅ FULL CONFIDENCE: n >= 7 (original logic)
        else:
            if med >= 32 and iqr <= 4:  return +3.0, tag
            if med >= 28 and iqr <= 5:  return +2.0, tag
            if med >= 24 and iqr <= 6:  return +1.0, tag
            if med < 16 or iqr >= 12:   return -3.0, tag
            if med < 20 or iqr >= 9:    return -2.0, tag
            return 0.0, tag
            
    except Exception as e:
        print(f"[MINUTES ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 0.0, ""

def is_high_variance_player(player_name: str) -> bool:
    """Check if player has high variance in recent minutes (unreliable for projections)."""
    try:
        pid = bdl_find_player_id(player_name)
        if not pid: 
            return False
        med, iqr, n = bdl_recent_minutes(pid, last_n=7)
        if n < 5:
            return True  # Not enough data = high uncertainty
        # High variance = IQR > 40% of median
        variance_ratio = iqr / max(1, med)
        return variance_ratio > 0.4
    except Exception:
        return True  # Error = assume high variance
    
# ========================= Team-level injury pressure =======================
def _injury_bucket(status: str) -> int:
    s = _norm(status or "")
    if "out" in s or "inactive" in s:      return 2
    if "doubt" in s:                        return 1
    return 0

_team_pressure_cache: Dict[str,int] = {}
_team_pressure_built_at = 0.0

def team_injury_pressure_map(team_filter: Optional[set[str]] = None) -> Dict[str, int]:
    """
    Calculate injury pressure scores for teams.
    
    Args:
        team_filter: Optional set of team codes to limit injury checks to
    """
    global _team_pressure_cache, _team_pressure_built_at

    # Only use cache if team filter hasn't changed
    cache_key = frozenset(team_filter) if team_filter else None
    if (_team_pressure_cache and 
        (time.time() - _team_pressure_built_at) < 120 and 
        _inj_index_for_ts == _injuries_fetched_at and
        getattr(team_injury_pressure_map, '_last_filter', None) == cache_key):
        return _team_pressure_cache

    injuries = fetch_nba_official_injuries()
    agg: Dict[str,int] = {}

    for row in injuries or []:
        tm_raw = (row.get("Team") or row.get("TeamName") or row.get("TeamAbbr") or "").strip()
        if not tm_raw:
            continue
        canon = team_key(tm_raw)
        if not canon:
            continue
        
        # ✅ SKIP if team filter is active and this team isn't in it
        if team_filter and canon not in team_filter:
            continue

        status_raw = row.get("InjuryStatus") or row.get("Status") or row.get("Injury")
        bucket = _injury_bucket(status_raw)
        if bucket == 0:
            continue

        nm = str(row.get("Name") or row.get("Player") or "")
        
        # ✅ Only check minutes for players on filtered teams
        print(f"[INJURY CHECK] {nm} ({canon})")  # Debug to see which players are checked
        
        pid = bdl_find_player_id(nm)
        med = 0.0
        if pid:
            try:
                med, _iqr, _n = bdl_recent_minutes(pid, last_n=7)
            except Exception:
                med = 0.0

        if med >= 32:   mult = 2.0
        elif med >= 28: mult = 1.6
        elif med >= 24: mult = 1.3
        elif med >= 18: mult = 1.0
        elif med > 0:   mult = 0.7
        else:           mult = 0.8

        score_add = int(round(bucket * mult))
        if score_add <= 0:
            score_add = 1

        agg[canon] = agg.get(canon, 0) + score_add

    _team_pressure_cache = agg
    _team_pressure_built_at = time.time()
    team_injury_pressure_map._last_filter = cache_key  # Store filter for cache validation
    
    return agg

# FIXED: Team injury pressure uses relative scaling + exponential curve
def team_pressure_scores(team_name: str, opponent_name: str, team_filter: Optional[set[str]] = None) -> tuple[int,int,int,float]:
    m = team_injury_pressure_map(team_filter=team_filter)  # ✅ Pass filter
    t = m.get(team_key(team_name), 0)
    o = m.get(team_key(opponent_name), 0)
    diff = o - t
    
    if t == 0 and o == 0:
        return 0, 0, 0, 0.0
    
    max_pressure = max(1, t, o)
    relative_diff = diff / max_pressure
    bump = math.tanh(relative_diff * 1.0) * 0.04
    bump = clamp(bump, -0.05, 0.05)
    
    return t, o, diff, bump

# ========================= Line mismatch & confidence =======================
def line_advantage(fd_line: float, other_lines: List[float], side: str) -> float:
    if not other_lines: return 0.0
    mean_other = sum(other_lines) / len(other_lines)
    return (mean_other - fd_line) if side in ("Over","Cover") else (fd_line - mean_other)

def count_worse_line(fd_line: float, other_lines: List[float], side: str) -> int:
    worse = 0
    for ol in other_lines:
        if side in ("Over","Cover") and (ol > fd_line + 1e-9): worse += 1
        if side in ("Under",) and (ol < fd_line - 1e-9): worse += 1
    return worse

def kelly_fraction(p_true: float, dec_odds: float) -> float:
    b = dec_odds - 1.0
    q = 1.0 - p_true
    try:
        return max(0.0, (b * p_true - q) / b) if b > 0 else 0.0
    except Exception:
        return 0.0

# FIXED: Confidence now incorporates all adjustments
def confidence_score_from_prob(true_prob: float, inj_adj: float = 0.0, 
                               min_adj: float = 0.0, steam_adj: float = 0.0) -> tuple[int, str]:
    """
    FIXED: Now applies injury, minutes, and steam adjustments to confidence.
    Old: only used raw true_prob
    New: adjusted_prob = true_prob + (inj_adj + min_adj + steam_adj)/100
    """
    # Apply adjustments (scaled to probability space)
    adjusted_prob = clamp(true_prob + (inj_adj + min_adj + steam_adj) * 0.01, 0.0, 1.0)
    adjusted_prob = clamp(adjusted_prob, 0.0, 1.0)
    
    conf = int(round(adjusted_prob * 100))
    hi, med, lo = BADGE_THRESHOLDS.get("HIGH", 70), BADGE_THRESHOLDS.get("MED", 60), BADGE_THRESHOLDS.get("LOW", 55)
    badge = "HIGH" if conf >= hi else ("MED" if conf >= med else ("LOW" if conf >= lo else "PASS"))
    return conf, badge

def plus_odds_confidence_score(true_prob: float, fd_odds: int, 
                                gap_cents: int, books_used: int,
                                inj_adj: float = 0.0, min_adj: float = 0.0) -> tuple[int, str]:
    """
    Confidence scoring for plus odds bets (underdogs/longshots).
    
    Plus odds logic:
    - Higher gap = more confident (books disagree with FD)
    - More books = more confident (broader consensus)
    - True prob > implied = mispriced underdog
    - Injury/minutes still matter but are weighted differently
    """
    
    # Base confidence from probability
    base_conf = true_prob * 100
    
    # Gap bonus (plus odds have bigger gaps naturally)
    gap_bonus = min(15, gap_cents / 2)  # Up to +15% for big gaps
    
    # Books bonus (need strong consensus for underdogs)
    books_bonus = min(10, (books_used - 3) * 2)  # Up to +10% for 8+ books
    
    # FD odds penalty (very long odds are risky)
    if fd_odds > 250:
        odds_penalty = (fd_odds - 250) / 20  # -1% per 20 cents over +250
    else:
        odds_penalty = 0
    
    # Apply adjustments (scaled for plus odds context)
    inj_scaled = inj_adj * 0.5  # Injury matters less for underdogs
    min_scaled = min_adj * 0.7  # Minutes matter less for underdogs
    
    adjusted_conf = base_conf + gap_bonus + books_bonus - odds_penalty + inj_scaled + min_scaled
    adjusted_conf = clamp(adjusted_conf, 0, 100)
    
    conf = int(round(adjusted_conf))
    
    # Badge thresholds (lower for plus odds since they're inherently riskier)
    badge = "HIGH" if conf >= 55 else ("MED" if conf >= 48 else ("LOW" if conf >= 42 else "PASS"))
    
    return conf, badge

def correlation_penalty(rows: List[Dict[str, Any]]) -> tuple[Dict[tuple, int], Dict[tuple, str]]:
    """Calculate correlation penalties with progressive scaling."""
    pen: Dict[tuple, int] = {}
    flag: Dict[tuple, str] = {}
    by_player: Dict[str, List[tuple]] = {}
    by_game: Dict[str, List[tuple]] = {}
    
    # Group by player and game
    for r in rows:
        key = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
        by_player.setdefault(r["Player"], []).append(key)
        by_game.setdefault(r["Matchup"], []).append(key)
    
    # Progressive player penalties (less aggressive)
    for player, keys in by_player.items():
        if len(keys) > 1:
            # Check if props are actually correlated
            markets = set()
            for k in keys:
                markets.add(k[2])  # Market type
            
            # Different markets = lower correlation
            if len(markets) > 1:
                penalty_mult = 0.5  # Half penalty for different stat types
            else:
                penalty_mult = 1.0  # Full penalty for same stat type
            
            for i, k in enumerate(keys):
                # Progressive penalty: 5, 10, 15, 20... instead of quadratic
                base_penalty = min(20, i * 5)
                pen[k] = pen.get(k, 0) + int(base_penalty * penalty_mult)
                flag[k] = f"P{len(keys)}"  # P2, P3, etc.
    
    # Game concentration penalties (more lenient)
    for game, keys in by_game.items():
        if len(keys) > 2:
            # Check diversity of bets in the game
            unique_players = len(set(k[1] for k in keys))
            unique_markets = len(set(k[2] for k in keys))
            
            # More diverse = lower penalty
            diversity_factor = min(1.0, (unique_players + unique_markets) / (len(keys) * 2))
            
            for k in keys:
                # Base penalty reduced, with diversity adjustment
                penalty = max(0, int((len(keys) - 2) * 3 * (1 - diversity_factor * 0.5)))
                pen[k] = pen.get(k, 0) + penalty
                
                existing_flag = flag.get(k, "")
                if not existing_flag.startswith("P"):
                    flag[k] = f"G{len(keys)}"
    
    # Set defaults
    for r in rows:
        k = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
        flag.setdefault(k, "OK")
        pen.setdefault(k, 0)
    
    return pen, flag

# ============================ Consensus & movement ==========================
# FIXED: Winsorization instead of trimming for small samples
def trimmed_weighted_mean(values, weights, trim=0.15):
    """
    Robust consensus using adaptive trimming and outlier detection.
    """
    if not values:
        return None
    
    # Ensure matching lengths
    if not isinstance(weights, (list, tuple)):
        weights = [float(weights)] * len(values)
    if len(weights) != len(values):
        m = min(len(values), len(weights))
        values = list(values)[:m]
        weights = list(weights)[:m]
    
    if not values:
        return None
    
    # Convert to floats
    try:
        values = [float(v) for v in values]
        weights = [float(w) for w in weights]
    except:
        return None
    
    n = len(values)
    
    # For very small samples, use robust median
    if n <= 3:
        return statistics.median(values)
    
    # Detect outliers using modified Z-score
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    
    if mad < 0.001:  # All values very similar
        return sum(v*w for v,w in zip(values, weights)) / sum(weights)
    
    # Modified Z-scores
    m_z_scores = [0.6745 * (v - median) / mad for v in values]
    
    # Mark outliers (modified Z-score > 2.5)
    outlier_threshold = 2.5
    cleaned_pairs = []
    outlier_count = 0
    
    for i, (v, w) in enumerate(zip(values, weights)):
        if abs(m_z_scores[i]) > outlier_threshold:
            outlier_count += 1
            # Don't completely exclude, but heavily downweight outliers
            cleaned_pairs.append((v, w * 0.1))
        else:
            cleaned_pairs.append((v, w))
    
    # If too many outliers, fall back to standard trimming
    if outlier_count > n * 0.4:
        pairs = sorted(zip(values, weights), key=lambda x: x[0])
        
        if n <= 6:
            # Winsorize for small samples
            if n >= 4:
                # Move extremes 25% toward center
                pairs[0] = (pairs[0][0] * 0.75 + pairs[1][0] * 0.25, pairs[0][1])
                pairs[-1] = (pairs[-1][0] * 0.75 + pairs[-2][0] * 0.25, pairs[-1][1])
        else:
            # Standard trimming for larger samples
            k = int(n * trim)
            if n - 2*k > 0:
                pairs = pairs[k:n-k]
        
        num = sum(v*w for v,w in pairs)
        den = sum(w for _,w in pairs)
        return (num/den) if den else None
    
    # Use cleaned weights
    num = sum(v*w for v,w in cleaned_pairs)
    den = sum(w for _,w in cleaned_pairs)
    return (num/den) if den else None

def book_weight(book_key: str) -> float:
    global BOOK_WEIGHTS
    if not isinstance(BOOK_WEIGHTS, dict):
        BOOK_WEIGHTS = DEFAULT_WEIGHTS.copy()
    try:
        return float(BOOK_WEIGHTS.get(book_key, 1.0))
    except Exception:
        return 1.0

def db_log_tick(rows_for_event: List[tuple]):
    _db_execmany("""INSERT INTO ticks
        (ts,event_id,matchup,tip_et,player,market,line,side,book,price)
        VALUES(?,?,?,?,?,?,?,?,?,?)""", rows_for_event)

def db_log_bets(bets_rows: List[tuple]):
    _db_execmany("""INSERT INTO bets
        (ts,event_id,matchup,tip_et,player,market,line,side,fd_price,fair_prob,true_prob,confidence,badge)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", bets_rows)

def last_10min_move(event_id: str, player: str, market: str, line: float, side: str):
    now = int(time.time()); since = now - 600
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""
        SELECT ts, book, price FROM ticks
        WHERE ts>=? AND event_id=? AND player=? AND market=? 
        AND line BETWEEN ? AND ? AND side=?
    """, (since, event_id, player, market, 
        float(line) - 0.01, float(line) + 0.01, side))
    rows = cur.fetchall(); con.close()
    if not rows: return 0, 0
    
    first, last = {}, {}
    for ts, book, price in rows:
        first.setdefault(book, (ts, price))
        if book not in last or ts > last[book][0]:
            last[book] = (ts, price)
    
    # ✅ Calculate probability change (not raw odds change)
    def prob_diff(p0, p1):
        try:
            prob0 = american_to_implied_prob(int(p0))
            prob1 = american_to_implied_prob(int(p1))
            # Return change in basis points (1% = 100 bps)
            return round((prob1 - prob0) * 10000)
        except:
            return 0
    
    fd_change, sharp_changes = 0, []
    for book, (t0, p0) in first.items():
        p1 = last[book][1]
        if book == FANDUEL_KEY:
            fd_change = prob_diff(p0, p1)
        elif book in SHARP_BOOKS:
            sharp_changes.append(prob_diff(p0, p1))
    
    sharp_avg = round(sum(sharp_changes)/len(sharp_changes)) if sharp_changes else 0
    return fd_change, sharp_avg

# FIXED: Steam window is now configurable by betting window
def steam_boost(event_id: str, player: str, market: str, line: float, side: str,
                window_sec: int = 1800, min_books: int = 3, sharp_set=None) -> float:
    """
    FIXED: Window is now passed from betting window preset.
    Old: Fixed 600s (10 min)
    New: Configurable (morning: 14400s/4hr, pretip: 1800s/30min)
        """
    sharp_set = sharp_set or SHARP_BOOKS
    now = int(time.time()); since = now - window_sec
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""
        SELECT ts, book, price FROM ticks
        WHERE ts>=? AND event_id=? AND player=? AND market=? AND ABS(line-?)<1e-6 AND side=?
    """, (since, event_id, player, market, float(line), side))
    rows = cur.fetchall(); con.close()
    if not rows: return 0.0

    earliest = {}; latest = {}
    for ts, book, price in rows:
        if book not in sharp_set:
            continue
        earliest.setdefault(book, (ts, price))
        if book not in latest or ts > latest[book][0]:
            latest[book] = (ts, price)

    total_move = 0
    books_moving_with = 0  # ✅ Only count books moving WITH the side
    for book in sharp_set:
        if book in earliest and book in latest:
            p0 = american_to_implied_prob(earliest[book][1])
            p1 = american_to_implied_prob(latest[book][1])
            delta = p1 - p0
            if delta > 0:  # ✅ Only positive movement
                total_move += delta
                books_moving_with += 1  # ✅ Count this book

    if books_moving_with == 0:  # ✅ Check books that actually moved
        return 0.0

    avg_move = total_move / books_moving_with  # ✅ Average of books moving WITH

    # More conservative: require 2% minimum move, scale moderately
    if avg_move < 0.02:
        return 0.0
        
    boost = (avg_move - 0.02) * 100 * 1.2  # 4% move = 2.4% boost
    return min(2.0, max(0.0, boost))
# =============================== Usage model (props) ========================
def rolling_player_mean(player_name: str, stat_key: str, n: int = 10) -> float:
    pid = bdl_find_player_id(player_name)
    if not pid: return 0.0
    url = "https://api.balldontlie.io/v1/stats"
    data = http_get_json(url, params={"player_ids[]": pid, "per_page": 100}, headers=bdl_headers())
    if not data or not isinstance(data, dict): return 0.0
    rows = data.get("data", [])
    rows.sort(key=lambda r: r.get("game", {}).get("date",""), reverse=True)
    vals = []
    for r in rows[:30]:
        v = r.get(stat_key, 0) or 0
        vals.append(float(v))
    if not vals: return 0.0
    short = statistics.mean(vals[:n]) if len(vals) >= n else statistics.mean(vals)
    long  = statistics.mean(vals)
    return 0.5*short + 0.5*long

def poisson_hit_prob(mean: float, line: float, side: str) -> float:
    if mean <= 0: return 0.0 if side == "Over" else 1.0
    
    # For Over/Under fractional lines (e.g., 24.5)
    k_floor = int(math.floor(line))
    
    def log_pmf(lmb, x):
        if x > 170:
            return -lmb + x * math.log(lmb) - x * math.log(x) + x
        return -lmb + x * math.log(lmb) - sum(math.log(i) for i in range(1, x + 1))
    
    if side == "Over":
        # P(X > line) = 1 - P(X <= floor(line))
        s = 0.0
        for x in range(0, k_floor + 1):
            s += math.exp(log_pmf(mean, x))
        return max(0.0, min(1.0, 1.0 - s))
    else:  # Under
        # P(X <= floor(line))
        s = 0.0
        for x in range(0, k_floor + 1):
            s += math.exp(log_pmf(mean, x))
        return max(0.0, min(1.0, s))
    
    k_threshold = int(math.floor(line))
    
    # ✅ Use log-space to avoid overflow
    def log_pmf(lmb, x):
        if x > 170:  # Factorial overflow risk
            # Use Stirling's approximation
            return -lmb + x * math.log(lmb) - x * math.log(x) + x
        return -lmb + x * math.log(lmb) - sum(math.log(i) for i in range(1, x + 1))
    
    max_k = int(max(60, mean + 6 * math.sqrt(max(1e-6, mean))))
    max_k = min(max_k, 200)  # ✅ Cap to prevent crazy loops
    
    if side == "Over":
        # For line 24.5, we need P(X > 24.5) = 1 - P(X ≤ 24)
        k_floor = int(math.floor(line))
        s = 0.0
        for x in range(0, k_floor + 1):
            s += math.exp(log_pmf(mean, x))
        return max(0.0, min(1.0, 1.0 - s))  # Return complement
    else:
        s = 0.0
        for x in range(0, k_threshold + 1):
            s += math.exp(log_pmf(mean, x))
        return min(1.0, max(0.0, s))
    
def get_player_variance_stats(player_name: str, stat_key: str, n: int = 10) -> tuple[float, float, float]:
    """Get mean, std dev, and coefficient of variation for a player's recent stats."""
    pid = bdl_find_player_id(player_name)
    if not pid: 
        return 0.0, 0.0, 1.0
    
    url = "https://api.balldontlie.io/v1/stats"
    data = http_get_json(url, params={"player_ids[]": pid, "per_page": 100}, headers=bdl_headers())
    if not data or not isinstance(data, dict): 
        return 0.0, 0.0, 1.0
    
    rows = data.get("data", [])
    rows.sort(key=lambda r: r.get("game", {}).get("date",""), reverse=True)
    
    vals = []
    mins_played = []
    for r in rows[:30]:
        v = r.get(stat_key, 0) or 0
        m = parse_min_to_float(r.get("min"))
        if m > 10:  # Only include games with meaningful minutes
            vals.append(float(v))
            mins_played.append(m)
    
    if len(vals) < 3:
        return 0.0, 0.0, 1.0
    
    # Get recent subset
    recent = vals[:n] if len(vals) >= n else vals
    
    mean = statistics.mean(recent)
    stdev = statistics.stdev(recent) if len(recent) > 1 else 0.0
    cv = stdev / mean if mean > 0 else 1.0
    
    # Check minutes consistency
    min_cv = statistics.stdev(mins_played[:n]) / statistics.mean(mins_played[:n]) if len(mins_played) >= n else 1.0
    
    # Adjust variance based on minutes inconsistency
    adjusted_cv = cv * (1 + min_cv * 0.5)
    
    return mean, stdev, adjusted_cv

def negative_binomial_hit_prob(mean: float, variance: float, line: float, side: str) -> float:
    """Use negative binomial for high-variance players, Poisson for consistent ones."""
    if mean <= 0: 
        return 0.0 if side == "Over" else 1.0
    
    # If variance <= mean, use Poisson
    if variance <= mean * 1.1:
        return poisson_hit_prob(mean, line, side)
    
    # Use negative binomial for overdispersed data
    # NB parameterization: r (successes), p (probability)
    p = mean / variance
    r = mean * mean / (variance - mean)
    
    if r <= 0 or p <= 0 or p >= 1:
        return poisson_hit_prob(mean, line, side)  # Fallback
    
    # For negative binomial, we need to calculate the CDF manually
    # since scipy might not be available
    k_threshold = int(math.floor(line))
    
    # Simple approximation using normal distribution for large r
    if r > 30:
        nb_mean = r * (1 - p) / p
        nb_var = r * (1 - p) / (p * p)
        nb_std = math.sqrt(nb_var)
        
        # Continuity correction
        if side == "Over":
            z = (line + 0.5 - nb_mean) / nb_std
            # Approximate normal CDF
            prob = 0.5 * (1 + math.erf(-z / math.sqrt(2)))
        else:
            z = (line + 0.5 - nb_mean) / nb_std
            prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        return max(0.0, min(1.0, prob))
    
    # For small r, use the Poisson approximation
    return poisson_hit_prob(mean, line, side)

# =============================== Scanning logic =============================
def _process_one_event(evt: Dict[str, Any], selected_markets: List[str],
                       min_books: int, trim_used: float,
                       ml_bump_scale: float, spread_bump_scale: float,
                       min_ev: float,
                       status_cb,
                       window_mode: str,
                       require_ev: bool,
                       require_gap: bool,
                       min_gap_cents: int,
                       min_avg_gap_cents: int,
                       min_true_prob_pct: float,
                       steam_window_sec: int,
                       team_filter: Optional[set[str]] = None) -> tuple[list[Dict[str, Any]], Optional[str]]:

    home_raw, away_raw = evt["home"], evt["away"]
    matchup   = f"{away_raw} @ {home_raw}".strip()
    tip_short = fmt_time_short(evt["tip"])
    if status_cb:
        status_cb(f"Fetching markets for {matchup}")

    resp, h = fetch_event_props_retry(evt["id"], selected_markets)
    if h.get("error") or resp is None:
        return [], f"Skipping {matchup}: {h.get('error','Empty response')}"

    bookmakers = resp.get("bookmakers", []) if isinstance(resp, dict) else (resp if isinstance(resp, list) else [])
    if not isinstance(bookmakers, list) or not bookmakers:
        return [], f"No bookmakers for {matchup}"

    prices: Dict[tuple, Dict[str, Dict[str, int]]] = {}
    line_book_map: Dict[tuple, Dict[str, set]] = {}

    for bk in bookmakers:
        if not isinstance(bk, dict):
            continue
        bkey = (bk.get("key") or "").lower()
        if bkey not in {FANDUEL_KEY, *OTHER_BOOKS}:
            continue
        for m in bk.get("markets", []) or []:
            mkey = m.get("key")
            if mkey not in selected_markets:
                continue

            if mkey in ("player_points","player_rebounds","player_assists","player_threes"):
                for out in m.get("outcomes", []) or []:
                    side  = out.get("name")
                    line  = out.get("point")
                    price = out.get("price")
                    player = out.get("description") or out.get("participant") or out.get("player") or "Unknown"
                    if side not in ("Over","Under") or line is None or price is None:
                        continue
                    try:
                        line_f  = float(line)
                        price_i = int(price)
                    except Exception:
                        continue
                    k = (player, mkey, line_f)
                    prices.setdefault(k, {}).setdefault(bkey, {})[side] = price_i
                    lm_key = (player, mkey)
                    line_book_map.setdefault(lm_key, {}).setdefault(bkey, set()).add(line_f)

            elif mkey == "h2h":
                for out in m.get("outcomes", []) or []:
                    team = out.get("name") or out.get("description") or ""
                    price = out.get("price")
                    if not team or price is None:
                        continue
                    try:
                        price_i = int(price)
                    except Exception:
                        continue
                    k = (team, "h2h", 0.0)
                    prices.setdefault(k, {}).setdefault(bkey, {})["Win"] = price_i

            elif mkey == "spreads":
                for out in m.get("outcomes", []) or []:
                    team  = out.get("name") or out.get("description") or ""
                    price = out.get("price")
                    point = out.get("point")
                    if not team or price is None or point is None:
                        continue
                    try:
                        price_i = int(price)
                        line_f  = float(point)
                    except Exception:
                        continue
                    k = (team, "spreads", line_f)
                    prices.setdefault(k, {}).setdefault(bkey, {})["Cover"] = price_i
                    lm_key = (team, "spreads")
                    line_book_map.setdefault(lm_key, {}).setdefault(bkey, set()).add(line_f)

            elif mkey == "totals":
                for out in m.get("outcomes", []) or []:
                    side  = out.get("name")
                    price = out.get("price")
                    point = out.get("point")
                    if side not in ("Over","Under") or price is None or point is None:
                        continue
                    try:
                        price_i = int(price)
                        line_f  = float(point)
                    except Exception:
                        continue
                    k = ("TOTAL", "totals", line_f)
                    prices.setdefault(k, {}).setdefault(bkey, {})[side] = price_i
                    lm_key = ("TOTAL", "totals")
                    line_book_map.setdefault(lm_key, {}).setdefault(bkey, set()).add(line_f)

    tick_rows = []
    now_ts = int(time.time())
    for (player, market_key, line), book_map in prices.items():
        for b, sides in book_map.items():
            for s, p in (sides or {}).items():
                try:
                    tick_rows.append((now_ts, evt["id"], matchup, tip_short,
                                      player, market_key, float(line), s, b, int(p)))
                except Exception:
                    pass
    if tick_rows: db_log_tick(tick_rows)

    out_rows: List[Dict[str, Any]] = []

    for (who, market_key, line), book_map in prices.items():
        def _best_and_avg_gap(fd_price: int, side_key: str) -> tuple[int, Optional[str], Optional[int], int]:
            try:
                fd_price_i = int(fd_price)
            except Exception:
                fd_price_i = fd_price
            
            # ✅ FIXED: Add sign filter back
            fd_sign_positive = (fd_price_i >= 0)

            best_other_book: Optional[str] = None
            best_other_price: Optional[int] = None
            others: List[int] = []

            for b, sides in book_map.items():
                if b == FANDUEL_KEY:
                    continue
                if side_key not in sides:
                    continue
                try:
                    p = int(sides[side_key])
                except Exception:
                    continue

                # ✅ FIXED: Skip opposite-sign quotes
                if (p >= 0) != fd_sign_positive:
                    continue

                others.append(p)
                if best_other_book is None or price_better_for_bettor(p, best_other_price):
                    best_other_book, best_other_price = b, p

            cents_delta_best = cents_diff(fd_price_i, best_other_price) if best_other_book else 0
            cents_delta_avg = 0
            if others:
                avg_other = int(round(sum(others) / len(others)))
                cents_delta_avg = cents_diff(fd_price_i, avg_other)

            return cents_delta_best, best_other_book, best_other_price, cents_delta_avg

        # ---------------------------- PROPS ----------------------------
        if market_key in ("player_points","player_rebounds","player_assists","player_threes"):
            for side in ("Over","Under"):
                if FANDUEL_KEY not in book_map or side not in book_map[FANDUEL_KEY]:
                    continue
                fd_price = book_map[FANDUEL_KEY][side]

                # ✅ FIXED: Apply odds filter FIRST for props
                if window_mode in ("pretip", "plus_odds"):  # ✅ Add plus_odds
                    preset = WINDOW_PRESETS.get(window_mode, {})
                    mode = preset.get("mode")
                    if mode in ("confidence", "plus_odds"):  # ✅ Add plus_odds
                        fd_odds_min = preset.get("fd_odds_min", -999)
                        fd_odds_max = preset.get("fd_odds_max", 999)
                        if not (fd_odds_min <= fd_price <= fd_odds_max):
                            continue

                cents_delta, best_other_book, best_other_price, cents_delta_avg = _best_and_avg_gap(fd_price, side)

                fair_probs: List[float] = []
                fair_wgts:  List[float] = []
                contributors_side_count = 0
                for b, sides in book_map.items():
                    if side in sides:
                        contributors_side_count += 1
                    if b == FANDUEL_KEY: 
                        continue
                    if "Over" in sides and "Under" in sides:
                        p_over  = american_to_implied_prob(sides["Over"])
                        p_under = american_to_implied_prob(sides["Under"])
                        denom = p_over + p_under
                        if denom > 0:
                            fair_over  = p_over / denom
                            fair_under = 1.0 - fair_over
                            fair_probs.append(fair_over if side == "Over" else fair_under)
                            fair_wgts.append(book_weight(b))
                books_used = len(fair_probs)
                if books_used < min_books:
                    continue

                market_prob = trimmed_weighted_mean(fair_probs, fair_wgts, trim=trim_used)
                if market_prob is None:
                    continue

                if len(fair_probs) >= 4:
                    qs = sorted(fair_probs); q = statistics.quantiles(qs, n=4)
                    q1, q3 = q[0], q[2]; iqr = max(1e-6, q3-q1)
                elif len(fair_probs) == 3:
                    qs = sorted(fair_probs); iqr = max(1e-6, (qs[2]-qs[0]) * 0.5)
                else:
                    iqr = 0.20

                stat_key = {"player_points":"pts","player_rebounds":"reb","player_assists":"ast","player_threes":"fg3m"}.get(market_key)
                p_model = None
                cv = 0.0

                if stat_key:
                    mean, stdev, cv = get_player_variance_stats(who, stat_key, n=10)
                    
                    if cv > 0.6:
                        variance = stdev ** 2 if stdev > 0 else mean * 1.5
                        p_model = negative_binomial_hit_prob(mean, variance, line, side)
                        variance_penalty = min(0.15, cv * 0.1)
                        p_model = p_model * (1 - variance_penalty)
                    else:
                        mu = rolling_player_mean(who, stat_key, n=10)
                        p_model = poisson_hit_prob(mu, line, side)

                true_prob = market_prob

                if p_model is not None:
                    if cv > 0.6:
                        alpha_market = 0.90
                    elif cv > 0.4:
                        alpha_market = 0.85
                    else:
                        alpha_market = max(0.75, min(0.90, 0.90 - iqr/0.35))
                    
                    true_prob = alpha_market * market_prob + (1.0 - alpha_market) * p_model

                p_lo, p_hi = min(fair_probs), max(fair_probs)
                buffer = 0.05
                true_prob = max(p_lo - buffer, min(p_hi + buffer, true_prob))
                p_med = statistics.median(fair_probs)
                true_prob = min(true_prob, p_med + 0.10)
                true_prob = max(true_prob, p_med - 0.10)

                lm_key = (who, market_key)
                other_lines: List[float] = []
                if lm_key in line_book_map:
                    for b, st in line_book_map[lm_key].items():
                        if b == FANDUEL_KEY: continue
                        if not st: continue
                        try:
                            nearest = min(st, key=lambda L: abs(float(L) - float(line)))
                            other_lines.append(float(nearest))
                        except Exception:
                            pass
                adv = line_advantage(line, other_lines, side)
                worse_ct = count_worse_line(line, other_lines, side)

                # ✅ Calculate ALL adjustments BEFORE filtering
                inj_adj, inj_tag = injury_confidence_adjust(who)
                min_adj, min_tag = minutes_confidence_adjust(who)
                steam_adj = steam_boost(evt["id"], who, market_key, line, side, window_sec=steam_window_sec)

                alt_shape_bonus = 1.5 if (worse_ct >= 2 and adv > 0.25) else 0.0
                
                adj_tags_list = []
                if inj_tag:
                    adj_tags_list.append(inj_tag)
                if min_tag:
                    adj_tags_list.append(min_tag)
                if steam_adj > 0:
                    adj_tags_list.append("steam")
                adj_tags_str = ",".join(adj_tags_list)

                fd_dec = american_to_decimal(fd_price)
                ev_val = true_prob * fd_dec - 1.0
                ev_pct = round(ev_val * 100.0, 2)

                # ✅ Now filter AFTER we have all adjustments
                edge_ok = True
                if require_gap:
                    if window_mode == "morning":
                        edge_ok = (cents_delta >= min_gap_cents) and (cents_delta_avg >= min_avg_gap_cents)
                    else:
                        edge_ok = (cents_delta >= min_gap_cents)
                prob_ok = True
                if min_true_prob_pct > 0:
                    prob_ok = (round(true_prob * 100.0, 2) >= min_true_prob_pct)
                ev_ok = True if not require_ev else (ev_pct >= float(min_ev))

                if not (edge_ok and prob_ok and ev_ok):
                    continue

                # ✅ Calculate confidence WITH adjustments
                if window_mode == "plus_odds":  # ✅ NEW
                    conf, badge = plus_odds_confidence_score(
                        true_prob, fd_price, cents_delta, books_used,
                        inj_adj=inj_adj, min_adj=min_adj
                    )
                else:
                    conf, badge = confidence_score_from_prob(
                        true_prob, inj_adj=inj_adj, min_adj=min_adj, steam_adj=steam_adj
                    )
                    
                if conf < min_true_prob_pct:
                    continue  # Skip bets below confidence threshold

                k_frac = kelly_fraction(true_prob, fd_dec) * float(CURRENT_KELLY_MULT)
                kelly_pct = round(min(KELLY_CAP_PCT, max(0.0, k_frac * 100.0)), 2)

                row = {
                    "Matchup": matchup,
                    "Tip (ET)": tip_short,
                    "Player": who,
                    "Market": {"player_points": "Points", "player_rebounds": "Rebounds",
                               "player_assists": "Assists", "player_threes": "3PM"}[market_key],
                    "Market Key": market_key,
                    "Side": side,
                    "Line": float(line),
                    "FD Odds": int(fd_price),
                    "Best Gap (¢)": int(cents_delta),
                    "Best Book": best_other_book or "",
                    "Best Other": best_other_book or "",
                    "Other Odds": best_other_price if best_other_price is not None else "",
                    "Gap (¢)": int(cents_delta),
                    "Avg Gap (¢)": int(cents_delta_avg),
                    "Line Adv": round(adv, 2),
                    "Worse Line Ct": int(worse_ct),
                    "Books Used": int(books_used),
                    "Fair Prob %": round(market_prob * 100.0, 2),
                    "True Prob %": round(true_prob * 100.0, 2),
                    "EV %": ev_pct,
                    "Confidence": int(conf),
                    "Badge": badge,
                    "Kelly %": kelly_pct,
                    "Team Inj": "",
                    "Injury": inj_tag if inj_tag else "",
                    "Min Med / IQR": min_tag if min_tag else "",
                    "Adj Tags": adj_tags_str,
                    "Event ID": evt["id"],
                    "event_id": evt["id"],
                }
                out_rows.append(row)

        # ---------------------------- MONEYLINE ----------------------------
        elif market_key == "h2h":
            if FANDUEL_KEY not in book_map or "Win" not in book_map[FANDUEL_KEY]:
                continue
            fd_price = book_map[FANDUEL_KEY]["Win"]
            opp = away_raw if who == home_raw else home_raw

            # ✅ FIXED: Apply odds filter FIRST for moneyline
            if window_mode == "pretip":
                preset = WINDOW_PRESETS.get(window_mode, {})
                if preset.get("mode") == "confidence":
                    fd_odds_min = preset.get("fd_odds_min", -999)
                    fd_odds_max = preset.get("fd_odds_max", 999)
                    if not (fd_odds_min <= fd_price <= fd_odds_max):
                        continue

            fair_probs, fair_wgts = [], []
            contributors = 0
            opp_key = (opp, "h2h", 0.0)
            opp_map = prices.get(opp_key, {})

            for b, sides in book_map.items():
                if b == FANDUEL_KEY:
                    continue
                p_self = sides.get("Win")
                p_opp = opp_map.get(b, {}).get("Win")
                if p_self is None or p_opp is None:
                    continue
                po = american_to_implied_prob(p_self)
                qo = american_to_implied_prob(p_opp)
                denom = po + qo
                if denom <= 0:
                    continue
                fair = po / denom
                fair_probs.append(fair)
                fair_wgts.append(book_weight(b))
                contributors += 1

            if contributors < max(2, min_books - 0):
                continue

            market_prob = trimmed_weighted_mean(fair_probs, fair_wgts, trim=trim_used)
            if market_prob is None:
                continue

            _t, _o, _diff, bump = team_pressure_scores(who, opp, team_filter=team_filter)
            true_prob = clamp(market_prob + bump * (WINDOW_PRESETS.get(window_mode, {}).get("ml_bump_scale", 1.0)), 0.0, 1.0)

            def _best_and_avg_gap_ml(fd_price_i: int) -> tuple[int, Optional[str], Optional[int], int]:
                fd_sign_positive = (int(fd_price_i) >= 0)
                
                best_other_book = None
                best_other_price = None
                others = []
                
                for b, sides in book_map.items():
                    if b == FANDUEL_KEY:
                        continue
                    if "Win" not in sides:
                        continue
                    try:
                        p = int(sides["Win"])
                    except Exception:
                        continue
                    
                    if (p >= 0) != fd_sign_positive:
                        continue
                    
                    others.append(p)
                    if best_other_book is None or price_better_for_bettor(p, best_other_price):
                        best_other_book, best_other_price = b, p
                
                cents_delta_best = cents_diff(int(fd_price_i), best_other_price) if best_other_book else 0
                cents_delta_avg = 0
                if others:
                    avg_other = int(round(sum(others) / len(others)))
                    cents_delta_avg = cents_diff(int(fd_price_i), avg_other)
                
                return cents_delta_best, best_other_book, best_other_price, cents_delta_avg

            cents_delta, best_other_book, best_other_price, cents_delta_avg = _best_and_avg_gap_ml(fd_price)

            edge_ok = True
            if require_gap:
                if window_mode == "morning":
                    edge_ok = (cents_delta >= min_gap_cents) and (cents_delta_avg >= min_avg_gap_cents)
                else:
                    edge_ok = (cents_delta >= min_gap_cents)

            prob_ok = (round(true_prob * 100.0, 2) >= min_true_prob_pct) if min_true_prob_pct > 0 else True
            fd_dec = american_to_decimal(fd_price)
            ev_pct = round((true_prob * fd_dec - 1.0) * 100.0, 2)
            ev_ok = True if not require_ev else (ev_pct >= float(min_ev))
            
            if not (edge_ok and prob_ok and ev_ok):
                continue

            conf, badge = confidence_score_from_prob(true_prob, inj_adj=0.0, min_adj=0.0, steam_adj=0.0)
            if conf < min_true_prob_pct:
                continue  # Skip bets below confidence threshold
            k_frac = kelly_fraction(true_prob, fd_dec) * float(CURRENT_KELLY_MULT)
            kelly_pct = round(min(KELLY_CAP_PCT, max(0.0, k_frac * 100.0)), 2)

            row = {
                "Matchup": matchup,
                "Tip (ET)": tip_short,
                "Player": who,
                "Market": "Moneyline",
                "Market Key": "h2h",
                "Side": "Win",
                "Line": 0.0,
                "FD Odds": int(fd_price),
                "Best Gap (¢)": int(cents_delta),
                "Best Book": best_other_book or "",
                "Best Other": best_other_book or "",
                "Other Odds": best_other_price if best_other_price is not None else "",
                "Gap (¢)": int(cents_delta),
                "Avg Gap (¢)": int(cents_delta_avg),
                "Line Adv": 0.0,
                "Worse Line Ct": 0,
                "Books Used": int(contributors),
                "Fair Prob %": round(market_prob * 100.0, 2),
                "True Prob %": round(true_prob * 100.0, 2),
                "EV %": ev_pct,
                "Confidence": int(conf),
                "Badge": badge,
                "Kelly %": kelly_pct,
                "Team Inj": f"+{_t} / +{_o} (Δ{_diff:+d})" if (_t or _o) else "",
                "Injury": "",
                "Min Med / IQR": "",
                "Adj Tags": "team-pressure" if abs(bump) > 1e-6 else "",
                "Event ID": evt["id"],
                "event_id": evt["id"],
            }
            out_rows.append(row)

        # ---------------------------- SPREADS ----------------------------
        elif market_key == "spreads":
            if FANDUEL_KEY not in book_map or "Cover" not in book_map[FANDUEL_KEY]:
                continue
            fd_price = book_map[FANDUEL_KEY]["Cover"]
            opp = away_raw if who == home_raw else home_raw

            # ✅ FIXED: Apply odds filter FIRST for spreads
            if window_mode == "pretip":
                preset = WINDOW_PRESETS.get(window_mode, {})
                if preset.get("mode") == "confidence":
                    fd_odds_min = preset.get("fd_odds_min", -999)
                    fd_odds_max = preset.get("fd_odds_max", 999)
                    if not (fd_odds_min <= fd_price <= fd_odds_max):
                        continue

            fair_probs, fair_wgts = [], []
            contributors = 0
            opp_key = (opp, "spreads", -float(line))
            opp_map_exact = prices.get(opp_key, {})

            def _opp_price_for_book(bk: str) -> Optional[int]:
                if bk in opp_map_exact and "Cover" in opp_map_exact[bk]:
                    return opp_map_exact[bk]["Cover"]
                opp_family = {k: v for (t, m, ln), v in prices.items() if m == "spreads" and t == opp and abs(ln + float(line)) <= 0.5}
                if not opp_family:
                    return None
                m = None
                for (t, mkey, ln), v in opp_family.items():
                    if bk in v and "Cover" in v[bk]:
                        if m is None or abs(ln + float(line)) < m[0]:
                            m = (abs(ln + float(line)), v[bk]["Cover"])
                return m[1] if m else None

            for b, sides in book_map.items():
                if b == FANDUEL_KEY:
                    continue
                p_self = sides.get("Cover")
                p_opp = _opp_price_for_book(b)
                if p_self is None or p_opp is None:
                    continue
                po = american_to_implied_prob(p_self)
                qo = american_to_implied_prob(p_opp)
                denom = po + qo
                if denom <= 0:
                    continue
                fair = po / denom
                fair_probs.append(fair)
                fair_wgts.append(book_weight(b))
                contributors += 1

            if contributors < min_books:
                continue

            market_prob = trimmed_weighted_mean(fair_probs, fair_wgts, trim=trim_used)
            if market_prob is None:
                continue

            _t, _o, _diff, bump = team_pressure_scores(who, opp, team_filter=team_filter)
            bump *= WINDOW_PRESETS.get(window_mode, {}).get("spread_bump_scale", 1.0)
            true_prob = clamp(market_prob + bump, 0.0, 1.0)

            lm_key = (who, "spreads")
            other_lines = []
            if lm_key in line_book_map:
                for b, st in line_book_map[lm_key].items():
                    if b == FANDUEL_KEY or not st:
                        continue
                    try:
                        nearest = min(st, key=lambda L: abs(float(L) - float(line)))
                        other_lines.append(float(nearest))
                    except Exception:
                        pass
            adv = line_advantage(float(line), other_lines, side="Cover")
            worse_ct = count_worse_line(float(line), other_lines, side="Cover")

            def _best_and_avg_gap_sp(fd_price_i: int) -> tuple[int, Optional[str], Optional[int], int]:
                fd_sign_positive = (int(fd_price_i) >= 0)
                
                best_other_book = None
                best_other_price = None
                others = []
                
                for b, sides in book_map.items():
                    if b == FANDUEL_KEY:
                        continue
                    if "Cover" not in sides:
                        continue
                    try:
                        p = int(sides["Cover"])
                    except Exception:
                        continue
                    
                    if (p >= 0) != fd_sign_positive:
                        continue
                    
                    others.append(p)
                    if best_other_book is None or price_better_for_bettor(p, best_other_price):
                        best_other_book, best_other_price = b, p
                
                cents_delta_best = cents_diff(int(fd_price_i), best_other_price) if best_other_book else 0
                cents_delta_avg = 0
                if others:
                    avg_other = int(round(sum(others) / len(others)))
                    cents_delta_avg = cents_diff(int(fd_price_i), avg_other)
                
                return cents_delta_best, best_other_book, best_other_price, cents_delta_avg

            cents_delta, best_other_book, best_other_price, cents_delta_avg = _best_and_avg_gap_sp(fd_price)

            edge_ok = True
            if require_gap:
                if window_mode == "morning":
                    edge_ok = (cents_delta >= min_gap_cents) and (cents_delta_avg >= min_avg_gap_cents)
                else:
                    edge_ok = (cents_delta >= min_gap_cents)
            prob_ok = (round(true_prob * 100.0, 2) >= min_true_prob_pct) if min_true_prob_pct > 0 else True
            fd_dec = american_to_decimal(fd_price)
            ev_pct = round((true_prob * fd_dec - 1.0) * 100.0, 2)
            ev_ok = True if not require_ev else (ev_pct >= float(min_ev))
            
            if not (edge_ok and prob_ok and ev_ok):
                continue

            conf, badge = confidence_score_from_prob(true_prob, inj_adj=0.0, min_adj=0.0, steam_adj=0.0)
            if conf < min_true_prob_pct:
                continue  # Skip bets below confidence threshold
            k_frac = kelly_fraction(true_prob, fd_dec) * float(CURRENT_KELLY_MULT)
            kelly_pct = round(min(KELLY_CAP_PCT, max(0.0, k_frac * 100.0)), 2)

            row = {
                "Matchup": matchup,
                "Tip (ET)": tip_short,
                "Player": who,
                "Market": "Spread",
                "Market Key": "spreads",
                "Side": "Cover",
                "Line": float(line),
                "FD Odds": int(fd_price),
                "Best Gap (¢)": int(cents_delta),
                "Best Book": best_other_book or "",
                "Best Other": best_other_book or "",
                "Other Odds": best_other_price if best_other_price is not None else "",
                "Gap (¢)": int(cents_delta),
                "Avg Gap (¢)": int(cents_delta_avg),
                "Line Adv": round(adv, 2),
                "Worse Line Ct": int(worse_ct),
                "Books Used": int(contributors),
                "Fair Prob %": round(market_prob * 100.0, 2),
                "True Prob %": round(true_prob * 100.0, 2),
                "EV %": ev_pct,
                "Confidence": int(conf),
                "Badge": badge,
                "Kelly %": kelly_pct,
                "Team Inj": f"+{_t} / +{_o} (Δ{_diff:+d})" if (_t or _o) else "",
                "Injury": "",
                "Min Med / IQR": "",
                "Adj Tags": "team-pressure" if abs(bump) > 1e-6 else "",
                "Event ID": evt["id"],
                "event_id": evt["id"],
            }
            out_rows.append(row)

        # ---------------------------- TOTALS ----------------------------
        elif market_key == "totals":
            for side in ("Over", "Under"):
                if FANDUEL_KEY not in book_map or side not in book_map[FANDUEL_KEY]:
                    continue
                fd_price = book_map[FANDUEL_KEY][side]

                # ✅ FIXED: Apply odds filter FIRST for totals
                if window_mode == "pretip":
                    preset = WINDOW_PRESETS.get(window_mode, {})
                    if preset.get("mode") == "confidence":
                        fd_odds_min = preset.get("fd_odds_min", -999)
                        fd_odds_max = preset.get("fd_odds_max", 999)
                        if not (fd_odds_min <= fd_price <= fd_odds_max):
                            continue

                fair_probs, fair_wgts = [], []
                contributors = 0
                for b, sides in book_map.items():
                    if b == FANDUEL_KEY:
                        continue
                    if "Over" in sides and "Under" in sides:
                        p_over = american_to_implied_prob(sides["Over"])
                        p_under = american_to_implied_prob(sides["Under"])
                        denom = p_over + p_under
                        if denom <= 0:
                            continue
                        fair_over = p_over / denom
                        fair_probs.append(fair_over if side == "Over" else (1.0 - fair_over))
                        fair_wgts.append(book_weight(b))
                        contributors += 1

                if contributors < min_books:
                    continue

                market_prob = trimmed_weighted_mean(fair_probs, fair_wgts, trim=trim_used)
                if market_prob is None:
                    continue

                true_prob = market_prob

                def _best_and_avg_gap_tot(fd_price_i: int) -> tuple[int, Optional[str], Optional[int], int]:
                    fd_sign_positive = (int(fd_price_i) >= 0)
                    
                    best_other_book = None
                    best_other_price = None
                    others = []
                    
                    for b, sides in book_map.items():
                        if b == FANDUEL_KEY:
                            continue
                        if side not in sides:
                            continue
                        try:
                            p = int(sides[side])
                        except Exception:
                            continue
                        
                        if (p >= 0) != fd_sign_positive:
                            continue
                        
                        others.append(p)
                        if best_other_book is None or price_better_for_bettor(p, best_other_price):
                            best_other_book, best_other_price = b, p
                    
                    cents_delta_best = cents_diff(int(fd_price_i), best_other_price) if best_other_book else 0
                    cents_delta_avg = 0
                    if others:
                        avg_other = int(round(sum(others) / len(others)))
                        cents_delta_avg = cents_diff(int(fd_price_i), avg_other)
                    
                    return cents_delta_best, best_other_book, best_other_price, cents_delta_avg

                cents_delta, best_other_book, best_other_price, cents_delta_avg = _best_and_avg_gap_tot(fd_price)

                edge_ok = True
                if require_gap:
                    if window_mode == "morning":
                        edge_ok = (cents_delta >= min_gap_cents) and (cents_delta_avg >= min_avg_gap_cents)
                    else:
                        edge_ok = (cents_delta >= min_gap_cents)
                prob_ok = (round(true_prob * 100.0, 2) >= min_true_prob_pct) if min_true_prob_pct > 0 else True
                fd_dec = american_to_decimal(fd_price)
                ev_pct = round((true_prob * fd_dec - 1.0) * 100.0, 2)
                ev_ok = True if not require_ev else (ev_pct >= float(min_ev))
                
                if not (edge_ok and prob_ok and ev_ok):
                    continue

                conf, badge = confidence_score_from_prob(true_prob, inj_adj=0.0, min_adj=0.0, steam_adj=0.0)
                if conf < min_true_prob_pct:
                    continue  # Skip bets below confidence threshold
                k_frac = kelly_fraction(true_prob, fd_dec) * float(CURRENT_KELLY_MULT)
                kelly_pct = round(min(KELLY_CAP_PCT, max(0.0, k_frac * 100.0)), 2)

                row = {
                    "Matchup": matchup,
                    "Tip (ET)": tip_short,
                    "Player": "TOTAL",
                    "Market": "Total",
                    "Market Key": "totals",
                    "Side": side,
                    "Line": float(line),
                    "FD Odds": int(fd_price),
                    "Best Gap (¢)": int(cents_delta),
                    "Best Book": best_other_book or "",
                    "Best Other": best_other_book or "",
                    "Other Odds": best_other_price if best_other_price is not None else "",
                    "Gap (¢)": int(cents_delta),
                    "Avg Gap (¢)": int(cents_delta_avg),
                    "Line Adv": 0.0,
                    "Worse Line Ct": 0,
                    "Books Used": int(contributors),
                    "Fair Prob %": round(market_prob * 100.0, 2),
                    "True Prob %": round(true_prob * 100.0, 2),
                    "EV %": ev_pct,
                    "Confidence": int(conf),
                    "Badge": badge,
                    "Kelly %": kelly_pct,
                    "Team Inj": "",
                    "Injury": "",
                    "Min Med / IQR": "",
                    "Adj Tags": "",
                    "Event ID": evt["id"],
                    "event_id": evt["id"],
                }
                out_rows.append(row)

    return out_rows, None

def fetch_all_candidates(selected_markets: List[str], min_books: int, trim_used: float,
                         ml_bump_scale: float, spread_bump_scale: float,
                         min_ev: float,
                         progress_cb=None, status_cb=None,
                         # AI-CHANGE: plumb window rules down
                         window_mode: str = "pretip",
                         require_ev: bool = True,
                         require_gap: bool = False,
                         min_gap_cents: int = 0,
                         min_avg_gap_cents: int = 0,
                         min_true_prob_pct: float = 0.0,
                         steam_window_sec: int = 1800,           # NEW: per-window steam window
                         team_filter: Optional[set[str]] = None) -> List[Dict[str, Any]]:  # AI-CHANGE: added team_filter
    data, headers = fetch_featured_events()
    if data is None:
        raise RuntimeError(headers.get("error","Failed to fetch events."))
    if isinstance(data, dict):
        data = data.get("events", [])
    events = [{"id": ev.get("id"),
               "home": ev.get("home_team",""),
               "away": ev.get("away_team",""),
               "tip":  ev.get("commence_time","")} for ev in data]

    # --- Team filter at event level (AI-CHANGE 2025-11-09) ---
    if team_filter:
        canon_set = {team_key(t) for t in team_filter if t}
        events = [ev for ev in events
                  if (team_key(ev["home"]) in canon_set) or (team_key(ev["away"]) in canon_set)]

    total = len(events)
    if total == 0:
        return []

    candidates: List[Dict[str, Any]] = []
    done = 0
    if status_cb:
        status_cb(f"Fetching markets (0/{total})")

    # I/O concurrency
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(
            _process_one_event,
            evt, selected_markets, min_books, trim_used,
            ml_bump_scale, spread_bump_scale, min_ev,
            status_cb,
            window_mode, require_ev, require_gap,
            min_gap_cents, min_avg_gap_cents, min_true_prob_pct,
            steam_window_sec,
            team_filter  # ✅ ADD THIS
        ) for evt in events]
        for fut in as_completed(futures):
            rows, msg = fut.result()
            if msg and status_cb:
                status_cb(msg)
            candidates.extend(rows or [])
            done += 1
            if progress_cb:
                progress_cb(done, total)

    return candidates


def enforce_portfolio_caps(rows, max_per_game, max_per_player):
    by_game, by_player = {}, {}
    out = []
    for r in rows:
        g = r.get("Matchup","")
        p = r.get("Player","")
        if by_game.get(g,0) >= max_per_game:
            continue
        if by_player.get(p,0) >= max_per_player:
            continue
        out.append(r)
        by_game[g] = by_game.get(g,0)+1
        by_player[p] = by_player.get(p,0)+1
    return out


def scan_props(selected_markets: List[str], min_books: int, min_ev: float, bankroll: float, top_n: int,
               window_mode: str,
               progress_cb=None, status_cb=None,
               max_per_game: int = 2, max_per_player: int = 1,
               team_filter: Optional[set[str]] = None) -> List[Dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError("No Odds API key. Set ODDS_API_KEY or edit file.")

    preset = WINDOW_PRESETS.get(window_mode, WINDOW_PRESETS["pretip"])
    effective_min_books = max(1, int(min_books) + int(preset["min_books_delta"]))
    trim_used = float(preset["trim"])
    ml_bump_scale = float(preset["ml_bump_scale"])
    spread_bump_scale = float(preset["spread_bump_scale"])

    # Per-window rules (includes steam window)
    require_ev = bool(preset.get("require_ev", True))
    require_gap = bool(preset.get("require_gap", False))
    min_gap_cents = int(preset.get("min_gap_cents", 0))
    min_avg_gap_cents = int(preset.get("min_avg_gap_cents", 0))
    min_true_prob_pct = float(preset.get("min_true_prob_pct", 0))
    steam_window_sec = int(preset.get("steam_window_sec", 1800))  # NEW

    candidates = fetch_all_candidates(
        selected_markets, effective_min_books, trim_used, ml_bump_scale, spread_bump_scale,
        min_ev,
        progress_cb, status_cb,
        window_mode=window_mode,
        require_ev=require_ev,
        require_gap=require_gap,
        min_gap_cents=min_gap_cents,
        min_avg_gap_cents=min_avg_gap_cents,
        min_true_prob_pct=min_true_prob_pct,
        steam_window_sec=steam_window_sec,              # NEW
        team_filter=team_filter,
    )

    # Apply correlation flags/penalties
    pen_map, flag_map = correlation_penalty(candidates)

    final: List[Dict[str, Any]] = []
    bets_to_log: List[tuple] = []

    for r in candidates:
        # Get correlation penalty for this bet
        k = (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
        corr_pts = int(pen_map.get(k, 0))
        corr_flag = flag_map.get(k, "OK")

        # Recalculate Kelly with correlation penalty
        try:
            true_prob = float(r["True Prob %"]) / 100.0
            fd_dec = american_to_decimal(int(r["FD Odds"]))
        except Exception:
            true_prob, fd_dec = 0.0, 1.0

        # Apply correlation penalty to Kelly
        k_frac = kelly_fraction(true_prob, fd_dec) * float(CURRENT_KELLY_MULT)
        if not math.isfinite(k_frac) or k_frac < 0:
            k_frac = 0.0
        
        # Correlation haircut: each point ≈ 1% reduction, hard-capped at 50%
        haircut = clamp(1.0 - (corr_pts / 100.0), 0.5, 1.0)
        k_frac *= haircut
        k_frac = min(k_frac, KELLY_CAP_PCT / 100.0)
        kelly_pct = round(k_frac * 100.0, 2)
        
        # Update the Kelly % in the row with the correlation-adjusted value
        r["Kelly %"] = kelly_pct
        # Keys from _process_one_event
        # r has: "Badge","Confidence","Matchup","Tip (ET)","Player","Market","Side","Line",
        #        "FD Odds","Books Used","Fair Prob %","True Prob %","EV %","Best Book",
        #        "Best Gap (¢)","Avg Gap (¢)","Adj Tags","Event ID", ... (+optional per-market extras)
        if len(final) == 0:  # Just debug the first row
            print("\n=== DEBUG: First candidate from _process_one_event ===")
            print(f"Team Inj: '{r.get('Team Inj')}'")
            print(f"Injury: '{r.get('Injury')}'")
            print(f"Min Med / IQR: '{r.get('Min Med / IQR')}'")
            print(f"Adj Tags: '{r.get('Adj Tags')}'")
            print(f"Player: '{r.get('Player')}'")
            print(f"Market: '{r.get('Market')}'")
            print("===============================================\n")

        # Respect the already-adjusted confidence/badge from _process_one_event
        score = int(r.get("Confidence", 0))
        badge = r.get("Badge", "PASS")

        # Kelly with correlation haircut (Claude’s #8)
        try:
            p_true = float(r["True Prob %"]) / 100.0
            fd_dec = american_to_decimal(int(r["FD Odds"]))
        except Exception:
            p_true, fd_dec = 0.0, 1.0

        k_frac = kelly_fraction(p_true, fd_dec) * float(CURRENT_KELLY_MULT)
        if not math.isfinite(k_frac) or k_frac < 0:
            k_frac = 0.0
        # correlation haircut: each point ≈ 1% reduction, hard-capped at 50%
        haircut = clamp(1.0 - (corr_pts / 100.0), 0.5, 1.0)
        k_frac *= haircut
        k_frac = min(k_frac, KELLY_CAP_PCT / 100.0)
        kelly_pct = round(k_frac * 100.0, 2)

        # Map fields to what the table/export expects
        fair_prob = float(r.get("Fair Prob %", 0.0)) / 100.0 if r.get("Fair Prob %") not in ("", None) else 0.0
        fair_american = implied_prob_to_american(fair_prob) if fair_prob > 0 else ""

        best_other_book = r.get("Best Book", "")  # we didn’t store the exact other price; leave odds blank
        gap_cents = int(r.get("Best Gap (¢)", r.get("Gap (¢)", 0)))

        final_row = {
            # UI columns
            "Badge": badge,
            "Confidence": score,
            "Corr": corr_flag,
            "Matchup": r["Matchup"],
            "Tip (ET)": r["Tip (ET)"],
            "Player": r["Player"],
            "Market": r["Market"],
            "Side": r["Side"],
            "Line": r["Line"],
            "FD Odds": r["FD Odds"],
            "Fair Odds": fair_american,             # derived from Fair Prob %
            "True Prob %": r["True Prob %"],
            "EV %": r["EV %"],
            "Kelly %": kelly_pct,
            "Team Inj": r.get("Team Inj",""),       
            "Injury": r.get("Injury", ""),          # FIXED: Get from r, not hardcoded ""
            "Min Med / IQR": r.get("Min Med / IQR", ""),  # FIXED: Get from r, not hardcoded ""
            "Best Other": best_other_book,
            "Other Odds": r.get("Other Odds", ""),  # FIXED: Get from r, not hardcoded ""
            "Gap (¢)": gap_cents,
            "Books Used": r.get("Books Used", 0),
            # extras for logging/render helpers
            "event_id": r.get("Event ID", r.get("event_id","")),
        }

        # If _process_one_event gave us tags, split them into Injury/Minutes columns when possible
        # e.g. Adj Tags: "Q/GTD, 30.4/3.1, steam"
        tags = str(r.get("Adj Tags","")).split(",") if r.get("Adj Tags") else []
        tags = [t.strip() for t in tags if t.strip()]
        for t in tags:
            if "/" in t and any(ch.isdigit() for ch in t):   # minutes tag like "30.1/4.2"
                final_row["Min Med / IQR"] = t
            elif t.lower() in ("out","doubtful","q/gtd","probable"):
                final_row["Injury"] = t
            # else: leave other tags (e.g., "steam") implicit

        final.append(final_row)

    # Sort: confidence → EV (per preset’s spirit)
    final.sort(key=lambda rr: rr["Confidence"], reverse=True)

    final = enforce_portfolio_caps(final, max_per_game, max_per_player)
    final = final[:top_n]

    # Log chosen bets
    ts_now = int(time.time())
    bet_rows = []
    for r in final:
        try:
            bet_rows.append((
                ts_now, r["event_id"], r["Matchup"], r["Tip (ET)"], r["Player"], r["Market"],
                float(r["Line"]), r["Side"], int(r["FD Odds"]),
                None, float(r["True Prob %"])/100.0, int(r["Confidence"]), r["Badge"]
            ))
        except Exception:
            pass
    db_log_bets(bet_rows)

    return final

# ============================ Windows helpers ===============================
def enable_windows_dark_titlebar(tk_root):
    """
    Force dark (or dark grey) native titlebar on Win10/11 even when the system is in light mode.
    Uses UxTheme dark-mode opt-ins + DWM attributes. Safe no-op on non-Windows.
    """
    try:
        if sys.platform != "win32":
            return
        import ctypes
        from ctypes import wintypes

        hwnd = tk_root.winfo_id()

        # ---- Undocumented uxtheme dark-mode toggles (work on 1809+) ----
        uxtheme = ctypes.WinDLL("uxtheme", use_last_error=True)
        try:
            AllowDarkModeForApp    = uxtheme.AllowDarkModeForApp
            AllowDarkModeForWindow = uxtheme.AllowDarkModeForWindow
            RefreshPolicy          = uxtheme.RefreshImmersiveColorPolicyState
            AllowDarkModeForApp.argtypes    = [wintypes.BOOL]
            AllowDarkModeForWindow.argtypes = [wintypes.HWND, wintypes.BOOL]

            try: AllowDarkModeForApp(True)
            except Exception: pass
            try: AllowDarkModeForWindow(wintypes.HWND(hwnd), True)
            except Exception: pass
            try: RefreshPolicy()
            except Exception: pass
        except Exception:
            pass

        # ---- DWM flags/attrs ----
        dwm = ctypes.windll.dwmapi
        def _set_uint(attr, val):
            v = ctypes.c_uint(val)
            return dwm.DwmSetWindowAttribute(wintypes.HWND(hwnd), ctypes.c_uint(attr), ctypes.byref(v), ctypes.sizeof(v))
        def _set_int(attr, val):
            v = ctypes.c_int(val)
            return dwm.DwmSetWindowAttribute(wintypes.HWND(hwnd), ctypes.c_uint(attr), ctypes.byref(v), ctypes.sizeof(v))

        DWMWA_USE_IMMERSIVE_DARK_MODE_OLD = 19
        DWMWA_USE_IMMERSIVE_DARK_MODE     = 20
        DWMWA_BORDER_COLOR                = 34
        DWMWA_CAPTION_COLOR               = 35
        DWMWA_TEXT_COLOR                  = 36
        DWMWA_SYSTEMBACKDROP_TYPE         = 38  # Win11+

        if _set_int(DWMWA_USE_IMMERSIVE_DARK_MODE, 1) != 0:
            _set_int(DWMWA_USE_IMMERSIVE_DARK_MODE_OLD, 1)

        try:
            _set_uint(DWMWA_SYSTEMBACKDROP_TYPE, 2)
        except Exception:
            pass

        CAPTION = 0x001F1F1F  # #1f1f1f dark grey
        TEXT    = 0x00FFFFFF  # white
        BORDER  = 0x001F1F1F
        _set_uint(DWMWA_CAPTION_COLOR, CAPTION)
        _set_uint(DWMWA_TEXT_COLOR,    TEXT)
        _set_uint(DWMWA_BORDER_COLOR,  BORDER)

        try:
            tk_root.after(80,  lambda: (_set_int(DWMWA_USE_IMMERSIVE_DARK_MODE, 1),
                                        _set_uint(DWMWA_CAPTION_COLOR, CAPTION),
                                        _set_uint(DWMWA_TEXT_COLOR,    TEXT),
                                        _set_uint(DWMWA_BORDER_COLOR,  BORDER)))
            tk_root.after(300, lambda: (_set_int(DWMWA_USE_IMMERSIVE_DARK_MODE, 1),
                                        _set_uint(DWMWA_CAPTION_COLOR, CAPTION),
                                        _set_uint(DWMWA_TEXT_COLOR,    TEXT),
                                        _set_uint(DWMWA_BORDER_COLOR,  BORDER)))
        except Exception:
            pass
    except Exception:
        pass

def remove_minimize_button(tk_root):
    """Remove the minimize button on Windows while keeping normal chrome & Alt-Tab behavior."""
    try:
        if sys.platform != "win32":
            return
        import ctypes
        user32 = ctypes.windll.user32
        GWL_STYLE = -16
        WS_MINIMIZEBOX = 0x00020000
        SWP_NOSIZE = 0x0001
        SWP_NOMOVE = 0x0002
        SWP_NOZORDER = 0x0004
        SWP_FRAMECHANGED = 0x0020

        hwnd = tk_root.winfo_id()
        style = user32.GetWindowLongW(hwnd, GWL_STYLE)
        style &= ~WS_MINIMIZEBOX
        user32.SetWindowLongW(hwnd, GWL_STYLE, style)
        user32.SetWindowPos(hwnd, None, 0, 0, 0, 0,
                            SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_FRAMECHANGED)
    except Exception:
        pass

def center_on_screen(tk_root):
    try:
        tk_root.update_idletasks()
        w = tk_root.winfo_width() or 1400
        h = tk_root.winfo_height() or 780
        sw = tk_root.winfo_screenwidth()
        sh = tk_root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = (sh // 2) - (h // 2)
        tk_root.geometry(f"{w}x{h}+{x}+{y}")
    except Exception:
        pass

def make_windows_borderless(tk_root):
    """Remove native titlebar/borders on Windows but keep Alt-Tab/taskbar."""
    try:
        if sys.platform != "win32":
            tk_root.overrideredirect(True)
            return

        import ctypes
        from ctypes import wintypes

        GWL_STYLE     = -16
        GWL_EXSTYLE   = -20
        WS_CAPTION    = 0x00C00000
        WS_THICKFRAME = 0x00040000
        WS_MINIMIZEBOX= 0x00020000
        WS_MAXIMIZEBOX= 0x00010000
        WS_SYSMENU    = 0x00080000

        WS_EX_DLGMODALFRAME = 0x00000001
        WS_EX_WINDOWEDGE    = 0x00000100
        WS_EX_CLIENTEDGE    = 0x00000200
        WS_EX_STATICEDGE    = 0x00020000
        WS_EX_APPWINDOW     = 0x00040000

        user32 = ctypes.windll.user32

        GetWindowLongPtrW = user32.GetWindowLongPtrW
        SetWindowLongPtrW = user32.SetWindowLongPtrW
        SetWindowPos      = user32.SetWindowPos

        GetWindowLongPtrW.restype = ctypes.c_longlong if ctypes.sizeof(ctypes.c_void_p) == 8 else ctypes.c_long
        GetWindowLongPtrW.argtypes = [wintypes.HWND, ctypes.c_int]
        SetWindowLongPtrW.argtypes = [wintypes.HWND, ctypes.c_int, GetWindowLongPtrW.restype]

        SWP_NOSIZE        = 0x0001
        SWP_NOMOVE        = 0x0002
        SWP_NOZORDER      = 0x0004
        SWP_FRAMECHANGED  = 0x0020

        hwnd = tk_root.winfo_id()

        try: tk_root.overrideredirect(False)
        except Exception: pass

        style   = GetWindowLongPtrW(hwnd, GWL_STYLE)
        exstyle = GetWindowLongPtrW(hwnd, GWL_EXSTYLE)

        style &= ~WS_CAPTION
        style &= ~WS_THICKFRAME
        style &= ~WS_MINIMIZEBOX
        style &= ~WS_MAXIMIZEBOX
        style |=  WS_SYSMENU

        exstyle &= ~WS_EX_DLGMODALFRAME
        exstyle &= ~WS_EX_WINDOWEDGE
        exstyle &= ~WS_EX_CLIENTEDGE
        exstyle &= ~WS_EX_STATICEDGE
        exstyle |=  WS_EX_APPWINDOW

        SetWindowLongPtrW(hwnd, GWL_STYLE,   style)
        SetWindowLongPtrW(hwnd, GWL_EXSTYLE, exstyle)

        SetWindowPos(hwnd, None, 0, 0, 0, 0, SWP_NOSIZE | SWP_NOMOVE | SWP_NOZORDER | SWP_FRAMECHANGED)

    except Exception:
        try: tk_root.overrideredirect(True)
        except Exception: pass

def force_borderless(root):
    """Apply borderless now and re-apply shortly after to beat Windows repaint."""
    root.update_idletasks()
    make_windows_borderless(root)
    try:
        root.after(50,  lambda: make_windows_borderless(root))
        root.after(250, lambda: make_windows_borderless(root))
    except Exception:
        pass

def set_borderless(root, enabled: bool):
    """Turn borderless on/off at runtime. Keeps Alt-Tab/taskbar presence."""
    try:
        if sys.platform != "win32":
            try:
                root.overrideredirect(bool(enabled))
            except Exception:
                pass
            return

        import ctypes
        from ctypes import wintypes
        user32 = ctypes.windll.user32

        GWL_STYLE     = -16
        GWL_EXSTYLE   = -20
        WS_CAPTION    = 0x00C00000
        WS_THICKFRAME = 0x00040000
        WS_MINIMIZEBOX= 0x00020000
        WS_MAXIMIZEBOX= 0x00010000
        WS_SYSMENU    = 0x00080000

        WS_EX_DLGMODALFRAME = 0x00000001
        WS_EX_WINDOWEDGE    = 0x00000100
        WS_EX_CLIENTEDGE    = 0x00000200
        WS_EX_STATICEDGE    = 0x00020000
        WS_EX_APPWINDOW     = 0x00040000

        SWP_NOSIZE       = 0x0001
        SWP_NOMOVE       = 0x0002
        SWP_NOZORDER     = 0x0004
        SWP_FRAMECHANGED = 0x0020

        GetWindowLongPtrW = user32.GetWindowLongPtrW
        SetWindowLongPtrW = user32.SetWindowLongPtrW
        GetWindowLongPtrW.restype = ctypes.c_longlong if ctypes.sizeof(ctypes.c_void_p)==8 else ctypes.c_long
        GetWindowLongPtrW.argtypes = [wintypes.HWND, ctypes.c_int]
        SetWindowLongPtrW.argtypes = [wintypes.HWND, ctypes.c_int, GetWindowLongPtrW.restype]

        hwnd = root.winfo_id()
        style   = GetWindowLongPtrW(hwnd, GWL_STYLE)
        exstyle = GetWindowLongPtrW(hwnd, GWL_EXSTYLE)

        if enabled:
            style  &= ~WS_CAPTION
            style  &= ~WS_THICKFRAME
            style  &= ~WS_MINIMIZEBOX
            style  &= ~WS_MAXIMIZEBOX
            style  |=  WS_SYSMENU
            exstyle &= ~WS_EX_DLGMODALFRAME
            exstyle &= ~WS_EX_WINDOWEDGE
            exstyle &= ~WS_EX_CLIENTEDGE
            exstyle &= ~WS_EX_STATICEDGE
            exstyle |=  WS_EX_APPWINDOW
        else:
            style  |= (WS_CAPTION | WS_THICKFRAME | WS_MINIMIZEBOX | WS_MAXIMIZEBOX | WS_SYSMENU)
            exstyle |= (WS_EX_WINDOWEDGE | WS_EX_CLIENTEDGE | WS_EX_STATICEDGE | WS_EX_APPWINDOW)
            exstyle &= ~WS_EX_DLGMODALFRAME

        SetWindowLongPtrW(hwnd, GWL_STYLE,   style)
        SetWindowLongPtrW(hwnd, GWL_EXSTYLE, exstyle)
        user32.SetWindowPos(hwnd, None, 0, 0, 0, 0,
                            SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_FRAMECHANGED)
    except Exception:
        try:
            root.overrideredirect(bool(enabled))
        except Exception:
            pass

def enable_drag_move(drag_widget, tk_root):
    """Click-and-drag anywhere on drag_widget to move the borderless window."""
    def _start(e):
        tk_root._drag_x, tk_root._drag_y = e.x_root, e.y_root
        tk_root._win_x,  tk_root._win_y  = tk_root.winfo_x(), tk_root.winfo_y()
    def _drag(e):
        dx, dy = e.x_root - tk_root._drag_x, e.y_root - tk_root._drag_y
        tk_root.geometry(f"+{tk_root._win_x + dx}+{tk_root._win_y + dy}")
    drag_widget.bind("<Button-1>", _start)
    drag_widget.bind("<B1-Motion>", _drag)

def set_fullscreen(root, enabled: bool):
    """Enable/disable OS fullscreen. Stores flag on root as _is_fullscreen."""
    try:
        root.attributes("-fullscreen", bool(enabled))
    except Exception:
        try:
            root.state("zoomed" if enabled else "normal")
        except Exception:
            pass
    root._is_fullscreen = bool(enabled)

# =================================== GUI ====================================
class App:
    def __init__(self, root):
        self.root = root
        root.title("FanDuel +EV Finder — Weighted + Steam + Usage + Caps")

        # --- Gradient border on a canvas, with content inset ---
        self._border_px = 14
        self.canvas = tk.Canvas(root, highlightthickness=0, bd=0)
        self.canvas.pack(fill="both", expand=True)

        # Content frame inside the canvas
        self.frame = (tb.Frame if use_bootstrap else ttk.Frame)(self.canvas, padding=(12, 6, 12, 12))

        # No in-app fake titlebar; we use clean borderless fullscreen outside.
        self._content_win = self.canvas.create_window(
            self._border_px, self._border_px, anchor="nw", window=self.frame
        )

        self._bg_img = None
        self.canvas.bind("<Configure>", self._on_resize)

        self.work_q = queue.Queue()
        self.worker = None
        self.current_rows = []
        self.bankroll_cache = DEFAULT_BANKROLL

        # state
        self.market_vars: dict[str, "TkBooleanVar"] = {}
        for _, key in ALL_MARKETS:
            self.market_vars[key] = tk.BooleanVar(value=True)

        # numeric state (with requested defaults)
        self.ev_var = tk.DoubleVar(value=2.0)
        self.books_var = tk.IntVar(value=DEFAULT_MIN_BOOKS)
        self.topn_var = tk.IntVar(value=DEFAULT_TOP_N)
        self.bankroll_var = tk.DoubleVar(value=DEFAULT_BANKROLL)
        self.kelly_mult_var = tk.DoubleVar(value=0.5)
        self.max_game_var = tk.IntVar(value=3)
        self.max_player_var = tk.IntVar(value=3)

        # window mode
        self.window_var = tk.StringVar(value="pretip")
        # AI-NOTE: Window presets map. If you rename labels, keep map values stable.
        self._window_map = {
                "Morning (soft)": "morning", 
                "Pre-tip (high-confidence)": "pretip",
                "Plus Odds Hunter": "plus_odds"  # ✅ NEW
            }

        # --- Team filter state (AI-CHANGE 2025-11-09) ---
        # Accept comma-separated names/abbrevs; canonicalized via team_key; empty = no filter.
        self.team_filter_var = tk.StringVar(value="")                      # AI-CHANGE
        self._team_filter_set = set()  # set of canonical team codes      # AI-CHANGE

        self._build_toolbar(self.frame)
        self._build_table(self.frame)
        self._build_status(self.frame)

    # --- gradient helpers ---
    def _on_resize(self, event):
        w = max(200, int(self.canvas.winfo_width()))
        h = max(200, int(self.canvas.winfo_height()))
        b = self._border_px

        self.canvas.coords(self._content_win, b, b)
        try:
            self.canvas.itemconfigure(self._content_win, width=max(50, w - 2*b), height=max(50, h - 2*b))
        except Exception:
            pass

        self._render_gradient(w, h)

    def _render_gradient(self, w, h):
        # Deep-to-cherry red, mostly dark, subtle vignette + glossy highlight.
        if HAS_PIL:
            c0 = (12, 0, 4)      # very deep red (almost black-red)
            c1 = (235, 16, 46)   # candy cherry red (barely reached)

            base = 420
            gw = max(64, min(base, w))
            gh = max(64, min(base, h))

            img = Image.new("RGB", (gw, gh))
            px = img.load()

            def ease(t: float) -> float:
                return max(0.0, min(1.0, t ** 1.8))  # dark-biased

            for y in range(gh):
                vy = 1.0 - (y / (gh - 1))
                for x in range(gw):
                    vx = (x / (gw - 1))
                    t = 0.55 * vx + 0.45 * vy
                    t = ease(t)
                    r = int(c0[0] + (c1[0] - c0[0]) * t)
                    g = int(c0[1] + (c1[1] - c0[1]) * t)
                    b = int(c0[2] + (c1[2] - c0[2]) * t)
                    px[x, y] = (r, g, b)

            # Vignette
            vignette_strength = 0.35
            cx, cy = gw * 0.5, gh * 0.5
            max_d = (cx**2 + cy**2) ** 0.5
            px = img.load()
            for y in range(gh):
                for x in range(gw):
                    dx = (x - cx)
                    dy = (y - cy)
                    d = (dx*dx + dy*dy) ** 0.5
                    v = 1.0 - vignette_strength * (d / max_d) ** 1.25
                    v = max(0.65, min(1.0, v))
                    r, g, b = px[x, y]
                    px[x, y] = (int(r * v), int(g * v), int(b * v))

            # Convert for compositing
            img = img.convert("RGBA")

            # Gloss highlight (elliptical)
            gloss = Image.new("RGBA", (gw, gh), (255, 255, 255, 0))
            mask = Image.new("L", (gw, gh), 0)
            mpx = mask.load()

            gx, gy = int(gw * 0.35), int(gh * 0.22)
            rx, ry = gw * 0.48, gh * 0.36
            max_alpha = 90
            for y in range(gh):
                for x in range(gw):
                    nx = (x - gx) / rx
                    ny = (y - gy) / ry
                    r2 = nx*nx + ny*ny
                    if r2 <= 1.0:
                        a = int(max_alpha * (1.0 - r2) ** 1.8)
                        mpx[x, y] = max(mpx[x, y], a)

            gloss_overlay = Image.new("RGBA", (gw, gh), (255, 255, 255, 255))
            img = Image.alpha_composite(img, Image.composite(gloss_overlay, Image.new("RGBA", (gw, gh), (0, 0, 0, 0)), mask))

            # Upscale to canvas
            img = img.resize((max(1, w), max(1, h)), Image.LANCZOS)

            self._bg_img = ImageTk.PhotoImage(img)
            self.canvas.delete("bgimg")
            self.canvas.create_image(0, 0, anchor="nw", image=self._bg_img, tags="bgimg")
            self.canvas.lower("bgimg")
        else:
            self.canvas.delete("bgrect")
            self.canvas.create_rectangle(0, 0, w, h, fill="#0a0002", outline="", tags="bgrect")
            self.canvas.lower("bgrect")

    def _render_brand_image(self, text="NAO'S BETTOR", height_px=84, font_path=None):
        """
        Punk/grunge wordmark (KEPT):
        • Heavy black stroke (outline)
        • Dark→cherry red fill (matches border)
        • Subtle 'punky' roughness
        Returns a PhotoImage or None if PIL is unavailable.
        """
        if not HAS_PIL:
            return None

        from PIL import Image, ImageDraw, ImageFont, ImageTk, ImageFilter

        # ---------- font selection ----------
        candidates = []
        if font_path:
            candidates.append(font_path)
        candidates += [
            os.getenv("BRAND_FONT_PATH", "").strip(),
            os.path.join(os.path.dirname(__file__), "punk.ttf"),
            os.path.join(os.path.dirname(__file__), "assets", "punk.ttf"),
            # Windows fallbacks:
            r"C:\Windows\Fonts\CHILLER.TTF",
            r"C:\Windows\Fonts\IMPACT.TTF",
            r"C:\Windows\Fonts\AGENCYB.TTF",
            r"C:\Windows\Fonts\BAHNSCHRIFT.TTF",
        ]
        candidates = [p for p in candidates if p]

        font = None
        for fp in candidates:
            try:
                font = ImageFont.truetype(fp, size=height_px)
                break
            except Exception:
                pass
        if font is None:
            try:
                font = ImageFont.truetype("arialbd.ttf", size=height_px)
            except Exception:
                from PIL import ImageFont as IF
                font = IF.load_default()

        # ---------- sizing ----------
        stroke_w = max(3, height_px // 18)
        pad      = max(6, height_px // 10)

        tmp = Image.new("L", (10, 10), 0)
        td  = ImageDraw.Draw(tmp)
        bbox = td.textbbox((0, 0), text, font=font, stroke_width=stroke_w)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        W, H = tw + pad * 2, th + pad * 2
        y0   = (H - th) // 2

        # ---------- layers ----------
        stroke = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        fill   = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        mask   = Image.new("L",   (W, H), 0)

        # outline (BLACK)
        sd = ImageDraw.Draw(stroke)
        sd.text((pad, y0), text, font=font, fill=(0, 0, 0, 255),
                stroke_width=stroke_w, stroke_fill=(0, 0, 0, 255))

        # alpha mask (letter shapes only)
        md = ImageDraw.Draw(mask)
        md.text((pad, y0), text, font=font, fill=255)

        # gradient fill (deep → cherry red)
        c0 = (12, 0, 4)
        c1 = (235, 16, 46)
        gd = ImageDraw.Draw(fill)
        for y in range(H):
            t = y / max(1, H - 1)
            t = t * 0.8 if t < 0.35 else 0.28 + (t - 0.35) * 0.85
            r = int(c0[0] + (c1[0] - c0[0]) * t)
            g = int(c0[1] + (c1[1] - c0[1]) * t)
            b = int(c0[2] + (c1[2] - c0[2]) * t)
            gd.line([(0, y), (W, y)], fill=(r, g, b, 255))
        fill.putalpha(mask)

        # subtle roughness
        try:
            rough = mask.filter(ImageFilter.MaxFilter(3)).filter(ImageFilter.MinFilter(3)).filter(ImageFilter.GaussianBlur(0.5))
            rough_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            ImageDraw.Draw(rough_layer).bitmap((0, 0), rough, fill=(0, 0, 0, 40))
            stroke = Image.alpha_composite(stroke, rough_layer)
        except Exception:
            pass

        out = Image.alpha_composite(stroke, fill)
        return ImageTk.PhotoImage(out)
    
        # -------- Weights dialog (AI-CHANGE 2025-11-09) --------
    def on_weights(self):
        """
        Edit per-book weights used in the trimmed weighted consensus.
        Notes:
          • FanDuel is excluded from consensus; its weight is shown read-only.
          • Values are floats (e.g., 0.5, 1.0, 1.5). Invalid inputs revert to 1.0.
          • Saved to weights.json and applied immediately.
        """
        global BOOK_WEIGHTS

        top = tk.Toplevel(self.frame)
        top.title("Book Weights")
        frm = ttk.Frame(top, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)
        frm.columnconfigure(0, weight=0)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Set weights for contributor books (trimmed weighted mean).").grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 10)
        )

        # Build the editable list: all known books; FD shown but disabled.
        books_all = [FANDUEL_KEY] + [b for b in OTHER_BOOKS if b != FANDUEL_KEY]
        # Merge defaults with current so every key exists
        current = DEFAULT_WEIGHTS.copy()
        try:
            if isinstance(BOOK_WEIGHTS, dict):
                current.update({k: float(v) for k, v in BOOK_WEIGHTS.items()})
        except Exception:
            pass

        # Keep a var per row
        row_vars: Dict[str, "TkStringVar"] = {}

        def add_row(r, label, key, editable=True):
            ttk.Label(frm, text=label).grid(row=r, column=0, sticky="w", padx=(0, 10), pady=4)
            sv = tk.StringVar(value=str(current.get(key, 1.0)))
            ent = ttk.Entry(frm, textvariable=sv, width=10, state=("normal" if editable else "disabled"))
            ent.grid(row=r, column=1, sticky="w", pady=4)
            row_vars[key] = sv

        r = 1
        # FanDuel (read-only info)
        add_row(r, "fanduel (reference only)", FANDUEL_KEY, editable=False); r += 1
        ttk.Label(frm, text="(FanDuel is NOT used in consensus; shown for clarity.)")\
           .grid(row=r, column=0, columnspan=2, sticky="w", pady=(0, 10)); r += 1

        for b in books_all:
            if b == FANDUEL_KEY:
                continue
            add_row(r, b, b, editable=True); r += 1

        btns = ttk.Frame(frm); btns.grid(row=r, column=0, columnspan=2, sticky="e", pady=(12, 0))

        def _reset_defaults():
            for k, sv in row_vars.items():
                sv.set(str(DEFAULT_WEIGHTS.get(k, 1.0)))

        def _save():
            # Validate and persist
            new_map = DEFAULT_WEIGHTS.copy()
            for k, sv in row_vars.items():
                try:
                    v = float(str(sv.get()).strip())
                    if not math.isfinite(v) or v <= 0:
                        raise ValueError
                except Exception:
                    v = 1.0
                    sv.set("1.0")
                new_map[k] = v

            # Keep as dict; propagate to global and save file
            try:
                save_book_weights(new_map)
                BOOK_WEIGHTS = new_map
                self.set_status("Book weights saved.")
            except Exception as e:
                self.set_status(f"Could not save weights: {e}")
            top.destroy()

        ttk.Button(btns, text="Reset", command=_reset_defaults,
                   bootstyle=("secondary" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="Save", command=_save,
                   bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="Close", command=top.destroy,
                   bootstyle=("secondary" if use_bootstrap else None)).pack(side="left", padx=6)


    # -------- Toolbar (compact: left tools, center brand, right actions) --------
    def _build_toolbar(self, parent):
        bar = ttk.Frame(parent)
        bar.pack(fill="x", pady=(0, 2))

        left   = ttk.Frame(bar);  left.pack(side="left")
        center = ttk.Frame(bar);  center.pack(side="left", expand=True, fill="x")
        right  = ttk.Frame(bar);  right.pack(side="right")

        # Left controls
        ttk.Button(left, text="Markets…",  bootstyle=("secondary" if use_bootstrap else None),
                command=self._open_markets_dialog).pack(side="left", padx=6)
        ttk.Button(left, text="Filters…",  bootstyle=("secondary" if use_bootstrap else None),
                command=self._open_filters_dialog).pack(side="left", padx=6)
        ttk.Button(left, text="Bankroll…", bootstyle=("secondary" if use_bootstrap else None),
                command=self._open_bankroll_dialog).pack(side="left", padx=6)
        ttk.Button(left, text="Limits…",   bootstyle=("secondary" if use_bootstrap else None),
                command=self._open_limits_dialog).pack(side="left", padx=6)

        # AI-CHANGE: Teams filter button
        ttk.Button(left, text="Teams…",
                   bootstyle=("secondary" if use_bootstrap else None),
                   command=self._open_teams_dialog).pack(side="left", padx=6)  # AI-CHANGE

        ttk.Label(left, text="Betting Window").pack(side="left", padx=(12, 6))
        self.window_combo = ttk.Combobox(left, width=20, state="readonly",
                                        values=list(self._window_map.keys()))
        self.window_combo.set("Pre-tip (confirmed)")
        self.window_combo.pack(side="left")

        # Center brand (KEPT: punk wordmark with stroke + gradient)
        self.brand_img = self._render_brand_image("NAO'S BETTOR", height_px=84, font_path=BRAND_FONT_PATH)
        if self.brand_img is not None:
            ttk.Label(center, image=self.brand_img, anchor="center").pack(expand=True, pady=0)
        else:
            ttk.Label(center, text="NAO'S BETTOR", font=("Segoe UI", 16, "bold")).pack(expand=True, pady=0)

        # Right actions
        ttk.Button(right, text="Weights…", bootstyle=("secondary" if use_bootstrap else None),
                command=self.on_weights).pack(side="left", padx=6)
        self.search_btn = ttk.Button(right, text="Search",
                                    bootstyle=("danger" if use_bootstrap else None),
                                    command=self.on_search)
        self.search_btn.pack(side="left", padx=6)
        self.export_btn = ttk.Button(right, text="Export CSV",
                                    bootstyle=("danger" if use_bootstrap else None),
                                    state="disabled", command=self.on_export)
        self.export_btn.pack(side="left", padx=6)
        ttk.Button(right, text="Quit",
                bootstyle=("danger" if use_bootstrap else None),
                command=self.on_quit).pack(side="left", padx=6)
        
        # AI-CHANGE: Build Parlays from selected bets
        self.parlay_btn = ttk.Button(
            right, text="Parlays…",
            bootstyle=("danger" if use_bootstrap else None),
            command=self.on_build_parlays
        )
        self.parlay_btn.pack(side="left", padx=6)


        if BORDERLESS:
            enable_drag_move(bar, self.root)

    def on_quit(self):
        try:
            self.root.destroy()
        except Exception:
            pass

    # -------- Popups --------
    def _open_markets_dialog(self):
        top = tk.Toplevel(self.frame)
        top.title("Select Markets")
        frm = ttk.Frame(top, padding=12); frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Choose one or more markets:").pack(anchor="w", pady=(0, 8))

        row1 = ttk.Frame(frm); row1.pack(fill="x", pady=4)
        row2 = ttk.Frame(frm); row2.pack(fill="x", pady=4)

        CB = (tb.Checkbutton if use_bootstrap else ttk.Checkbutton)
        cb_style = ("danger" if use_bootstrap else None)

        for label, key in ALL_PROP_MARKETS:
            CB(row1, text=label, variable=self.market_vars[key],
            bootstyle=cb_style).pack(side="left", padx=6)

        for label, key in TEAM_MARKETS:
            CB(row2, text=label, variable=self.market_vars[key],
            bootstyle=cb_style).pack(side="left", padx=6)

        btns = ttk.Frame(frm); btns.pack(anchor="e", pady=(12, 0))
        ttk.Button(btns, text="All",
                command=lambda: self._set_all_markets(True),
                bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="None",
                command=lambda: self._set_all_markets(False),
                bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="Close",
                command=top.destroy,
                bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)

    def _set_all_markets(self, val: bool):
        for v in self.market_vars.values():
            try: v.set(bool(val))
            except Exception: pass

    def _open_filters_dialog(self):
        top = tk.Toplevel(self.frame)
        top.title("Filters")
        frm = ttk.Frame(top, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)
        frm.columnconfigure(0, weight=0)
        frm.columnconfigure(1, weight=1)

        def add_row(r, text, widget):
            ttk.Label(frm, text=text, anchor="w").grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            widget.grid(row=r, column=1, sticky="ew")

        ev_entry   = ttk.Entry(frm, width=10, textvariable=self.ev_var)
        books_spin = ttk.Spinbox(frm, from_=1, to=10, width=8, textvariable=self.books_var)
        topn_spin  = ttk.Spinbox(frm, from_=1, to=50, width=8, textvariable=self.topn_var)

        add_row(0, "Min EV %",  ev_entry)
        add_row(1, "Min Books", books_spin)
        add_row(2, "Top N",     topn_spin)

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=2, sticky="e", pady=(10,0))
        ttk.Button(btns, text="Close",
                command=top.destroy,
                bootstyle=("danger" if use_bootstrap else None)).pack(side="right")

    def _open_bankroll_dialog(self):
        top = tk.Toplevel(self.frame)
        top.title("Bankroll & Kelly")
        frm = ttk.Frame(top, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)
        frm.columnconfigure(0, weight=0)
        frm.columnconfigure(1, weight=1)

        def add_row(r, text, widget):
            ttk.Label(frm, text=text, anchor="w").grid(row=r, column=0, sticky="w", padx=(0,10), pady=6)
            widget.grid(row=r, column=1, sticky="ew")

        bk_entry = ttk.Entry(frm, width=12, textvariable=self.bankroll_var)
        k_spin   = ttk.Spinbox(frm, from_=0.1, to=1.0, increment=0.1, width=8, textvariable=self.kelly_mult_var)

        add_row(0, "Bankroll $", bk_entry)
        add_row(1, "Kelly ×",    k_spin)

        ttk.Label(frm, text=f"Note: Kelly stake is capped at {KELLY_CAP_PCT}% of bankroll.")\
            .grid(row=2, column=0, columnspan=2, sticky="w", pady=(6,0))

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=2, sticky="e", pady=(10,0))
        ttk.Button(btns, text="Close",
                command=top.destroy,
                bootstyle=("danger" if use_bootstrap else None)).pack(side="right")

    def _open_limits_dialog(self):
        top = tk.Toplevel(self.frame)
        top.title("Exposure Limits")
        frm = ttk.Frame(top, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)
        frm.columnconfigure(0, weight=0)
        frm.columnconfigure(1, weight=1)

        def add_row(r, text, widget):
            ttk.Label(frm, text=text, anchor="w").grid(row=r, column=0, sticky="w", padx=(0,10), pady=6)
            widget.grid(row=r, column=1, sticky="ew")

        max_game   = ttk.Spinbox(frm, from_=1, to=10, width=8, textvariable=self.max_game_var)
        max_player = ttk.Spinbox(frm, from_=1, to=10, width=8, textvariable=self.max_player_var)

        add_row(0, "Max/Game",   max_game)
        add_row(1, "Max/Player", max_player)

        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=2, sticky="e", pady=(10,0))
        ttk.Button(btns, text="Close",
                command=top.destroy,
                bootstyle=("danger" if use_bootstrap else None)).pack(side="right")

    # -------- Teams dialog (AI-CHANGE 2025-11-09) --------
    def _open_teams_dialog(self):
        """
        Teams filter dialog.
        Accepts comma-separated team names or abbreviations (e.g., "BOS, NYK, Celtics, Lakers").
        We canonicalize using team_key(...) so "Celtics" → "BOS", "Lakers" → "LAL", etc.
        Empty input = no filter (show all games).
        """
        top = tk.Toplevel(self.frame)
        top.title("Teams Filter")
        frm = ttk.Frame(top, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Only show games for these teams (comma-separated):").grid(row=0, column=0, columnspan=2, sticky="w")
        entry = ttk.Entry(frm, textvariable=self.team_filter_var)
        entry.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 2))
        ttk.Label(frm, text="Examples: BOS, NYK, Celtics, Lakers").grid(row=2, column=0, columnspan=2, sticky="w", pady=(0, 8))

        def _save():
            raw = (self.team_filter_var.get() or "").strip()
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            # Canonicalize to NBA codes via team_key(...)
            self._team_filter_set = {team_key(p) for p in parts}
            # Also keep user raw upper-cased to be permissive with unknown aliases.
            self._team_filter_set |= {p.upper() for p in parts}
            self.set_status(f"Teams filter: {', '.join(sorted(self._team_filter_set)) or 'ALL'}")
            top.destroy()

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=2, sticky="e")
        ttk.Button(btns, text="Clear",
                   command=lambda: (self.team_filter_var.set(""), self._team_filter_set.clear()),
                   bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="Save",
                   command=_save,
                   bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
        ttk.Button(btns, text="Close",
                   command=top.destroy,
                   bootstyle=("secondary" if use_bootstrap else None)).pack(side="left", padx=6)

    # -------- Table / Status --------
    def _build_table(self, parent):
        cols = ["Badge","Conf (Hit%)","Corr","Bet","FD","Fair","True %","EV %","Kelly %","Kelly $",
                "Move(10m)","Team Inj","Injury","Min Med / IQR","Best Other","Other","Gap¢","Books","Matchup","Tip"]
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", height=18)
        widths = [70,90,60,300,70,80,70,70,70,80,110,100,90,110,100,80,60,70,220,80]
        anchors = ["center","center","center","w","center","center","center","center","center","center",
                   "center","center","center","center","center","center","center","center","w","center"]
        for (c,w,a) in zip(cols, widths, anchors):
            self.tree.heading(c, text=c, command=lambda col=c: self._sort_by(col, False))
            self.tree.column(c, width=w, anchor=a)
        self.tree.pack(fill="both", expand=True, pady=(2, 8))
        if use_bootstrap:
            tb.Style().configure("Treeview", rowheight=28)

        self.tree.tag_configure("HIGH", foreground="#22c55e")
        self.tree.tag_configure("MED",  foreground="#f59e0b")
        self.tree.tag_configure("LOW",  foreground="#9ca3af")
        self.tree.tag_configure("PASS", foreground="#ef4444")

        self.tree.tag_configure("HIGH_BG", background="#0f2f1a")
        self.tree.tag_configure("MED_BG",  background="#2a2010")
        self.tree.tag_configure("LOW_BG",  background="#1f2429")
        self.tree.tag_configure("PASS_BG", background="#2b1313")

        try:
            if tkfont is not None:
                base = tkfont.nametofont("TkDefaultFont")
                bold = base.copy(); bold.configure(weight="bold")
                for t in ("HIGH","MED","LOW","PASS"):
                    self.tree.tag_configure(t, font=bold)
        except Exception:
            pass

    def _build_status(self, parent):
        bar = ttk.Frame(parent); bar.pack(fill="x")
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(bar, textvariable=self.status_var).pack(side="left")
        self.prog = (tb.Progressbar if use_bootstrap else ttk.Progressbar)(
            bar, mode="indeterminate", bootstyle="danger-striped" if use_bootstrap else ""
        )
        self.prog.pack(side="right", padx=6)
        self.prog.stop()

    # -------- Helpers --------
    def _sort_by(self, col, descending):
        data = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        def try_num(s):
            try: return float(str(s).replace("%","").replace("+","").replace("$","").replace("Δ",""))
            except: return s
        data.sort(key=lambda t: try_num(t[0]), reverse=descending)
        for idx, item in enumerate(data):
            self.tree.move(item[1], "", idx)
        self.tree.heading(col, command=lambda: self._sort_by(col, not descending))

    def set_status(self, text: str):
        self.status_var.set(text)

    # ===== Search flow =====
    def on_search(self):
        global CURRENT_KELLY_MULT
        if hasattr(self, "worker") and self.worker and self.worker.is_alive():
            return

        selected_markets = [k for k,v in self.market_vars.items() if v.get()]
        if not selected_markets:
            self.set_status("Select at least one market."); return

        try:
            min_ev = float(self.ev_var.get())
            bankroll = float(self.bankroll_var.get())
        except Exception:
            self.set_status("Enter valid numbers for Min EV and Bankroll."); return

        min_books = int(self.books_var.get())
        top_n = int(self.topn_var.get())
        self.bankroll_cache = bankroll
        CURRENT_KELLY_MULT = float(self.kelly_mult_var.get())

        label = self.window_combo.get()
        window_mode = self._window_map.get(label, "pretip")

        self.search_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.prog.start(15)
        self.set_status(f"Searching… ({label})")

        # AI-CHANGE: pass team filter into worker
        self.worker = threading.Thread(
            target=self._worker_search,
            args=(selected_markets, min_books, min_ev, bankroll, top_n, window_mode, self._team_filter_set.copy()),  # AI-CHANGE
            daemon=True
        )
        self.worker.start()
        self.after_poll()

    # AI-NOTE: Any exception path must enqueue ("error", msg) so UI doesn't hang with spinner.
    def _worker_search(self, selected_markets, min_books, min_ev, bankroll, top_n, window_mode, team_filter_set):  # AI-CHANGE: signature
        try:
            rows = scan_props(
                selected_markets=selected_markets,
                min_books=min_books,
                min_ev=min_ev,
                bankroll=bankroll,
                top_n=top_n,
                window_mode=window_mode,
                progress_cb=lambda i,t: self.work_q.put(("progress", f"{i}/{t}")),
                status_cb=lambda msg: self.work_q.put(("status", msg)),
                max_per_game=self.max_game_var.get(),
                max_per_player=self.max_player_var.get(),
                team_filter=team_filter_set,  # AI-CHANGE
            )
            self.work_q.put(("done", rows))
        except Exception as e:
            tb_txt = traceback.format_exc(limit=6)
            self.work_q.put(("error", f"{e.__class__.__name__}: {e}\n{tb_txt}"))

    # AI-NOTE: UI/worker ordering. Do not render rows until 'done' to avoid partial lists.
    def after_poll(self):
        try:
            while True:
                kind, payload = self.work_q.get_nowait()
                if kind == "progress":
                    self.set_status(f"Fetching events… {payload}")
                elif kind == "status":
                    self.set_status(payload)
                elif kind == "done":
                    self._load_rows(payload)
                    self.prog.stop()
                    self.search_btn.config(state="normal")
                    self.export_btn.config(state="normal" if payload else "disabled")
                    self.set_status(f"Done. Showing {len(payload)} picks.")
                    return
                elif kind == "error":
                    self.prog.stop()
                    self.search_btn.config(state="normal")
                    self.export_btn.config(state="disabled")
                    self.set_status(f"Error: {payload}")
                    if use_bootstrap: Messagebox.show_error(payload, "Error")
                    else: messagebox.showerror("Error", payload)
                    return
        except queue.Empty:
            pass
        self.frame.after(150, self.after_poll)

    def _load_rows(self, rows: List[Dict[str, Any]]):

        # if rows:
            # print("\n=== DEBUG: First row keys ===")
            # print(rows[0].keys())
            # print("\n=== DEBUG: First row sample values ===")
            # print(f"Best Other: {rows[0].get('Best Other')}")
            # print(f"Other Odds: {rows[0].get('Other Odds')}")
            # print(f"Team Inj: {rows[0].get('Team Inj')}")
            # print(f"Injury: {rows[0].get('Injury')}")
            # print(f"Min Med / IQR: {rows[0].get('Min Med / IQR')}")
            # print(f"Gap (¢): {rows[0].get('Gap (¢)')}")
            # print(f"Adj Tags: {rows[0].get('Adj Tags')}")
            # print("========================\n")
            
        for child in self.tree.get_children(""): 
            self.tree.delete(child)
        self.current_rows = rows
        
        for r in rows:
            try:
                fd_odds  = int(r["FD Odds"])
                true_p   = float(r["True Prob %"]) / 100.0
                fd_dec   = american_to_decimal(fd_odds)
                ev_check = round((true_p * fd_dec - 1.0) * 100.0, 2)
                if abs(float(r["EV %"]) - ev_check) > 1.0:
                    r["EV %"] = ev_check
            except Exception:
                pass

            mshort_map = {
                "player_points":"PTS","player_rebounds":"REB","player_assists":"AST","player_threes":"3PM",
                "h2h":"ML","spreads":"SPREAD","totals":"TOTAL"
            }
            mshort = mshort_map.get(r.get("Market Key", r["Market"]), r["Market"])

            if r["Market"] == "Moneyline":
                bet = f'{r["Player"]} ML'
            elif r["Market"] == "Spread":
                bet = f'{r["Player"]} {float(r["Line"]):+g} SPREAD'
            elif r["Market"] == "Total":
                bet = f'{r["Side"]} {r["Line"]} TOTAL'
            else:
                side_symbol = "o" if r["Side"] == "Over" else "u"
                bet = f'{r["Player"]} {side_symbol}{r["Line"]} {mshort}'

            kelly_dollars = round(self.bankroll_cache * (r["Kelly %"]/100.0), 2)
            market_key_map = {
                "Points": "player_points",
                "Rebounds": "player_rebounds", 
                "Assists": "player_assists",
                "3PM": "player_threes",
                "Moneyline": "h2h",
                "Spread": "spreads",
                "Total": "totals"
            }
            market_key = market_key_map.get(r["Market"], r.get("Market Key", ""))

            fd_mv, sharp_mv = last_10min_move(
                r.get("event_id","") or r.get("Event ID",""),
                r["Player"], 
                market_key,  # ✅ Use API key
                float(r["Line"]), 
                r["Side"]
            )
            fd_pct = fd_mv / 100.0  # 150 bps = 1.5%
            sharp_pct = sharp_mv / 100.0
            move_str = f'FD {fd_pct:+.1f}% / Shrp {sharp_pct:+.1f}%'
            
            # Extract injury and minutes from Adj Tags if not already set
            injury_status = r.get("Injury", "")
            min_med_iqr = r.get("Min Med / IQR", "")
            
            if not injury_status or not min_med_iqr:
                tags = str(r.get("Adj Tags","")).split(",") if r.get("Adj Tags") else []
                tags = [t.strip() for t in tags if t.strip()]
                for t in tags:
                    if "/" in t and any(ch.isdigit() for ch in t):   # minutes tag like "30.1/4.2"
                        if not min_med_iqr:
                            min_med_iqr = t
                    elif t.lower() in ("out","doubtful","q/gtd","probable"):
                        if not injury_status:
                            injury_status = t
            
            # Format Other Odds with + sign if it exists
            other_odds_display = ""
            if r.get("Other Odds") not in ("", None):
                try:
                    other_odds_display = f'{int(r["Other Odds"]):+d}'
                except:
                    other_odds_display = str(r.get("Other Odds", ""))
            
            values = [
                r["Badge"],
                r["Confidence"],
                r.get("Corr","OK"),
                bet,
                f'{int(r["FD Odds"]):+d}',
                f'{int(r.get("Fair Odds", 0)):+d}' if r.get("Fair Odds") not in ("", None, 0) else "",
                f'{float(r["True Prob %"]):.2f}',
                f'{float(r["EV %"]):.2f}',
                f'{float(r["Kelly %"]):.2f}',
                f'${kelly_dollars:.2f}',
                move_str,
                r.get("Team Inj", ""),
                injury_status,
                min_med_iqr,
                r.get("Best Other", ""),
                other_odds_display,
                int(r.get("Gap (¢)", 0)),
                int(r["Books Used"]),
                r["Matchup"],
                r["Tip (ET)"],
            ]
            badge = r["Badge"]
            self.tree.insert("", "end", values=values, tags=(badge, badge + "_BG"))

        # ===================== Parlay workflow (AI-CHANGE) ======================
    def _get_selected_bets(self) -> List[Dict[str, Any]]:
        """Return list of current_rows for the selected Treeview items (multi-select allowed)."""
        out = []
        sel = self.tree.selection()
        if not sel:
            return out
        # Build a lookup map from visible row values to backing dict
        # We’ll match by a stable tuple of visible columns.
        idx_map = {}
        for r in self.current_rows:
            bet_key = (
                r.get("Matchup",""), r.get("Player",""), r.get("Market",""),
                r.get("Side",""), float(r.get("Line", 0.0)), int(r.get("FD Odds", 0))
            )
            idx_map.setdefault(bet_key, []).append(r)

        cols = ["Matchup","Player","Market","Side","Line","FD"]  # these are the headings in the tree
        for iid in sel:
            vals = self.tree.item(iid, "values")
            try:
                match = vals[18]  # "Matchup"
                bettxt= vals[3]   # "Bet" (contains player/side/line/market already)
                fd    = int(str(vals[4]).replace("+","").replace("−","-"))
                # Re-derive keys from columns so we can map back to current_rows:
                # Because Bet text differs by market, parse minimally using our rendering rules:
                # We stored full fields in columns too, so prefer them:
                # columns order in _build_table: ["Badge","Conf (Hit%)","Corr","Bet","FD","Fair","True %","EV %","Kelly %","Kelly $",
                #   "Move(10m)","Team Inj","Injury","Min Med / IQR","Best Other","Other","Gap¢","Books","Matchup","Tip"]
                # We’ll pick from self.current_rows by closest key below:
                # Try to scan candidates in current_rows with same Matchup and FD.
                candidates = [r for r in self.current_rows if r.get("Matchup","")==match and int(r.get("FD Odds",0))==fd]
                # Narrow further by Player/Market/Side/Line using bet text tokens:
                pick = None
                for r in candidates:
                    # Construct our human string the same way _load_rows did for matching
                    if r["Market"] == "h2h":
                        expected = f'{r["Player"]} ML'
                    elif r["Market"] == "spreads":
                        expected = f'{r["Player"]} {float(r["Line"]):+g} SPREAD'
                    elif r["Market"] == "totals":
                        expected = f'{r["Side"]} {r["Line"]} TOTAL'
                    else:
                        side_symbol = "o" if r["Side"] == "Over" else "u"
                        short = {"player_points":"PTS","player_rebounds":"REB","player_assists":"AST","player_threes":"3PM"}.get(r["Market"], r["Market"])
                        expected = f'{r["Player"]} {side_symbol}{r["Line"]} {short}'
                    if expected == bettxt:
                        pick = r; break
                if pick is None and candidates:
                    pick = candidates[0]  # best-effort fallback
                if pick:
                    out.append(pick)
            except Exception:
                pass
        return out

    def _build_parlay_lists(self, picks: List[Dict[str, Any]], top_k_pairs: int = 20, top_k_triples: int = 20):
        """
        Given selected straights (picks), return two lists:
          pairs  = [(legs, p_hit, dec_odds, ev_pct), ...] sorted best-first
          triples= [(legs, p_hit, dec_odds, ev_pct), ...] sorted best-first

        Rules:
          - Skip any combo that repeats the same (Matchup, Player, Market, Side, Line).
          - For 'too-same' player duplicates across legs → skip.
          - Apply independence discount via _parlay_independence_discount.
        """
        # Dedup identical legs from selection
        def _sig(r):
            return (r["Matchup"], r["Player"], r["Market"], r["Side"], r["Line"])
        seen = set()
        cleaned = []
        for r in picks:
            key = _sig(r)
            if key not in seen:
                seen.add(key)
                cleaned.append(r)
        picks = cleaned

        pairs = []
        triples = []

        # --- Build pairs (Safe) ---
        for a, b in itertools.combinations(picks, 2):
            # No same-player doubles
            if a["Player"] == b["Player"]:
                continue
            legs = [a, b]
            p, dec, ev = _parlay_metrics(legs)
            pairs.append((legs, p, dec, ev))
        # Safe list: sort by hit probability desc, tie-break EV desc
        pairs.sort(key=lambda x: (x[1], x[3]), reverse=True)
        pairs = pairs[:top_k_pairs]

        # --- Build triples (Aggressive) ---
        if len(picks) >= 3:
            for a, b, c in itertools.combinations(picks, 3):
                # No same-player triples
                if len({a["Player"], b["Player"], c["Player"]}) < 3:
                    continue
                legs = [a, b, c]
                p, dec, ev = _parlay_metrics(legs)
                triples.append((legs, p, dec, ev))
            # Aggressive list: sort by EV desc, tie-break hit probability desc
            triples.sort(key=lambda x: (x[3], x[1]), reverse=True)
            triples = triples[:top_k_triples]

        return pairs, triples

    def _open_parlays_window(self, pairs, triples):
        """Show a modal window listing the parlays with copy/export options."""
        top = tk.Toplevel(self.frame)
        top.title("Suggested Parlays")
        wrap = ttk.Frame(top, padding=12); wrap.pack(fill="both", expand=True)
        nb = ttk.Notebook(wrap); nb.pack(fill="both", expand=True)

        def _make_tree(parent, title):
            frame = ttk.Frame(parent); parent.add(frame, text=title)
            cols = ["Type","Legs","Parlay Hit %","Parlay EV %","Parlay Dec Odds"]
            tree = ttk.Treeview(frame, columns=cols, show="headings", height=16)
            widths = [90, 700, 110, 110, 140]
            anchors= ["center","w","center","center","center"]
            for (c,w,a) in zip(cols, widths, anchors):
                tree.heading(c, text=c, command=lambda col=c, t=tree: self._sort_tree(t, col))
                tree.column(c, width=w, anchor=a)
            tree.pack(fill="both", expand=True)
            btns = ttk.Frame(frame); btns.pack(anchor="e", pady=(8,0))
            ttk.Button(btns, text="Copy CSV", command=lambda t=tree: self._copy_parlay_csv(t),
                       bootstyle=("secondary" if use_bootstrap else None)).pack(side="left", padx=6)
            ttk.Button(btns, text="Export CSV", command=lambda t=tree: self._export_parlay_csv(t),
                       bootstyle=("danger" if use_bootstrap else None)).pack(side="left", padx=6)
            return tree

        tree_pairs   = _make_tree(nb, "2-Leg Safe")
        tree_triples = _make_tree(nb, "3-Leg Aggressive")

        def _fmt_legs(legs):
            pieces = []
            for r in legs:
                market = r.get("Market Key", r["Market"])  # Prefer API key
                if market == "h2h":
                    s = f'{r["Player"]} ML ({int(r["FD Odds"]):+d})'
                elif market == "spreads":
                    s = f'{r["Player"]} {float(r["Line"]):+g} SPREAD ({int(r["FD Odds"]):+d})'
                elif market == "totals":
                    s = f'{r["Side"]} {r["Line"]} TOTAL ({int(r["FD Odds"]):+d})'
                else:  # Props
                    sym = "o" if r["Side"] == "Over" else "u"
                    short = {
                        "player_points":"PTS",
                        "player_rebounds":"REB",
                        "player_assists":"AST",
                        "player_threes":"3PM"
                    }.get(market, market)
                    s = f'{r["Player"]} {sym}{r["Line"]} {short} ({int(r["FD Odds"]):+d})'
                pieces.append(s)
            return "  •  ".join(pieces)

        for legs, p, dec, ev in pairs:
            tree_pairs.insert("", "end", values=(
                "Safe 2-leg",
                _fmt_legs(legs),
                f"{p*100:.2f}",
                f"{ev:.2f}",
                f"{dec:.4f}",
            ))
        for legs, p, dec, ev in triples:
            tree_triples.insert("", "end", values=(
                "Aggressive 3-leg",
                _fmt_legs(legs),
                f"{p*100:.2f}",
                f"{ev:.2f}",
                f"{dec:.4f}",
            ))

        # If nothing built, hint user
        if not pairs and not triples:
            self.set_status("Select at least two picks to build parlays.")

    def _sort_tree(self, tree: "TtkTreeview", col: str, descending: Optional[bool]=None):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        def try_num(s):
            try: return float(str(s))
            except: return s
        # toggle direction by remembering it on the widget
        tag = f"_sort_{col}"
        current = getattr(tree, tag, False) if descending is None else descending
        data.sort(key=lambda t: try_num(t[0]), reverse=not current)
        for i, item in enumerate(data):
            tree.move(item[1], "", i)
        setattr(tree, tag, not current)

    def _tree_to_csv(self, tree: "TtkTreeview") -> str:
        cols = tree["columns"]
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        for iid in tree.get_children(""):
            row = [tree.set(iid, c) for c in cols]
            w.writerow(row)
        return buf.getvalue()

    def _copy_parlay_csv(self, tree: "TtkTreeview"):
        try:
            txt = self._tree_to_csv(tree)
            self.root.clipboard_clear()
            self.root.clipboard_append(txt)
            self.set_status("Parlay list copied to clipboard.")
        except Exception as e:
            self.set_status(f"Copy failed: {e}")

    def _export_parlay_csv(self, tree: "TtkTreeview"):
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fn = f"parlays_{ts}.csv"
            with open(fn, "w", newline="", encoding="utf-8") as f:
                f.write(self._tree_to_csv(tree))
            self.set_status(f"Exported: {fn}")
        except Exception as e:
            self.set_status(f"Export failed: {e}")

    def on_build_parlays(self):
        """Entry point from the toolbar button."""
        picks = self._get_selected_bets()
        if len(picks) < 2:
            self.set_status("Select at least two picks in the table first.")
            if use_bootstrap:
                Messagebox.show_info("Select at least two picks in the table, then click Parlays…", "No Selection")
            else:
                messagebox.showinfo("No Selection", "Select at least two picks in the table, then click Parlays…")
            return

        # Defaults: 20 safe pairs, 20 aggressive triples (quick to compute & skim)
        pairs, triples = self._build_parlay_lists(picks, top_k_pairs=20, top_k_triples=20)
        self._open_parlays_window(pairs, triples)


    def on_export(self):
        if not self.current_rows:
            self.set_status("Nothing to export."); return
        n = self._write_csv(self.current_rows, "fanduel_topN_confidence.csv")
        self.set_status(f"Exported: fanduel_topN_confidence.csv ({n})")

    def _write_csv(self, rows: List[Dict[str, Any]], fn: str) -> int:
        keys = ["Badge","Confidence","Corr","Matchup","Tip (ET)","Player","Market","Side","Line",
                "FD Odds","Fair Odds","True Prob %","EV %","Team Inj","Injury","Min Med / IQR",
                "Best Other","Other Odds","Gap (¢)","Books Used","Kelly %"]
        with open(fn, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows:
                out = {k: r.get(k, "") for k in keys}
                try: out["FD Odds"] = int(out["FD Odds"]) if out["FD Odds"] != "" else ""
                except Exception: pass
                try: out["Fair Odds"] = int(out["Fair Odds"]) if out["Fair Odds"] != "" else ""
                except Exception: pass
                w.writerow(out)
        return len(rows)

def main():
    try:
        root = tb.Window(themename="darkly") if use_bootstrap else tk.Tk()
    except Exception:
        root = tk.Tk()

    root.geometry("1400x780")
    root.minsize(1100, 640)

    try: root.withdraw()
    except Exception: pass

    # Build UI first (so dark titlebar can re-apply post-build when needed)
    app = App(root)

    # Apply brand theme to controls (buttons/progressbar)
    apply_brand_styles(root)

    # Dark titlebar (safe no-op off Windows)
    enable_windows_dark_titlebar(root)

    # === KEPT: Start in fullscreen + borderless (brand-first, full-bleed) ===
    # AI-INVARIANT (UX): Borderless + Fullscreen behavior: start fullscreen and reassert once.
    if START_FULLSCREEN:
        set_borderless(root, True)
        set_fullscreen(root, True)
        root.after(120, lambda: set_fullscreen(root, True))  # re-assert once

    # Center (visible if fullscreen falls back to maximized)
    center_on_screen(root)

    # --- Hotkeys ---
    def _toggle_full(event=None):
        if getattr(root, "_is_fullscreen", False):
            # EXIT FULLSCREEN → keep borderless + maximize (no native caption flash)
            set_fullscreen(root, False)
            set_borderless(root, True)
            try:
                root.update_idletasks()
                root.state("normal")
                root.state("zoomed")
            except Exception:
                pass
            try:
                root.after(60,  lambda: set_borderless(root, True))
                root.after(240, lambda: set_borderless(root, True))
            except Exception:
                pass
            try:
                root.update_idletasks()
                w = max(1200, root.winfo_width()); h = max(700, root.winfo_height())
                root.geometry(f"{w}x{h}+{root.winfo_x()}+{root.winfo_y()}")
            except Exception:
                pass
        else:
            # ENTER FULLSCREEN
            set_borderless(root, True)
            set_fullscreen(root, True)
            try:
                root.after(120, lambda: set_fullscreen(root, True))
            except Exception:
                pass

    def _exit_full_on_esc(event=None):
        if getattr(root, "_is_fullscreen", False):
            _toggle_full()

    def _minimize(event=None):
        try:
            root.iconify()
        except Exception:
            pass

    root.bind("<F11>", _toggle_full)
    root.bind("<Alt-Return>", _toggle_full)
    root.bind("<Escape>", _exit_full_on_esc)  # Esc exits fullscreen (does NOT quit)
    root.bind("<Control-m>", _minimize)

    try: root.deiconify()
    except Exception: pass

    root.mainloop()

def test_minutes():
    """Test minutes fetching for a known player"""
    print("\n" + "="*60)
    print("TESTING MINUTES FETCH")
    print("="*60)
    
    test_players = ["LeBron James", "Stephen Curry", "Giannis Antetokounmpo"]
    
    for player in test_players:
        print(f"\n[TEST] Checking {player}...")
        adj, tag = minutes_confidence_adjust(player)
        print(f"[TEST] Result: adj={adj}, tag={tag}")
        print()


if __name__ == "__main__":
    #test_minutes()
    main()