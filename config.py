"""
Configuration for NBA Bettor data directory
Edit DATA_DIR to point to your "Bettor Website" folder
"""

import os
import pathlib

# ========== EDIT THIS PATH ==========
# Option 1: Use a subdirectory called "data"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Option 2: Use your "Bettor Website" folder (Windows example)
# DATA_DIR = r"C:\Users\YourName\Code Shenanigans\Bettor Website"

# Option 3: Use your "Bettor Website" folder (Mac/Linux example)
# DATA_DIR = "/Users/YourName/Code Shenanigans/Bettor Website"
# ====================================

# Create directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

# File paths
WEIGHTS_PATH = pathlib.Path(DATA_DIR) / "weights.json"
DB_PATH = pathlib.Path(DATA_DIR) / "market_ticks.sqlite"

print(f"[CONFIG] Data directory: {DATA_DIR}")
print(f"[CONFIG] Database: {DB_PATH}")
print(f"[CONFIG] Weights: {WEIGHTS_PATH}")