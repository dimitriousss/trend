"""
Configuration file for Allegro Poland Demand Sensing System
"""
import os
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "demand_sensing.db"

# Create directories
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Database
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

# TikTok Configuration
TIKTOK_HASHTAGS = [
    "desksetup",
    "homeoffice",
    "desksetupinspo",
    "studiosetup",
    "workspaceaesthetic",
    "gadgetsdesk"
]

# Instagram Configuration
INSTAGRAM_HASHTAGS = [
    "desksetup",
    "workspaceinspo",
    "homeofficeideas"
]

# Allegro Configuration
ALLEGRO_KEYWORDS = [
    "monitor light bar",
    "rgb desk light",
    "headphone stand",
    "desk shelf",
    "cable organizer",
    "magnetic phone stand",
    "LED clock",
    "desk mat"
]

# Scraping settings
RATE_LIMIT_DELAY = 2  # seconds between requests
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# TikTok API settings (if using unofficial API)
TIKTOK_API_BASE = "https://www.tiktok.com"

# Allegro settings
ALLEGRO_BASE_URL = "https://allegro.pl"
ALLEGRO_TOP_N_LISTINGS = 20  # Number of top listings to analyze

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"