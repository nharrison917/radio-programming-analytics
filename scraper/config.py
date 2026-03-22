from dotenv import load_dotenv
import os

load_dotenv()

# --- Monitoring thresholds ---

MIN_PLAYS_PER_HOUR = 3
MAX_PLAYS_PER_HOUR = 30
FLAG_NULL_STATION_SHOW = True
FLAG_SUSPICIOUS_TITLE = True

# Shows known to play little or no music - suppress low play count warnings
LOW_PLAY_SUPPRESSED_SHOWS = {
    "Your Finances with Ken Mahoney",
}

# If a play title contains any of these strings, suppress low play count
# warnings for that hour AND the following hour
LOW_PLAY_SUPPRESSED_TITLE_SIGNALS = [
    "Anything, Anything with Rich Russo",
]

# -------------------

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SCRAPER_CONTACT = os.getenv("SCRAPER_CONTACT")

BASE_PLAYED_URL = "https://www.1071thepeak.com/played/"

HEADERS = {
    "User-Agent": f"PeakRadioScraper/1.0 (Academic Research; contact: {SCRAPER_CONTACT})"
}

DB_PATH = "radio_plays.db"

CRAWL_DELAY = 8 # Respect robots.txt
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF = 5

DISALLOWED_PATH_FRAGMENTS = [
    "/artist/",
    "/searchresults/",
    "/_hp/"
]
if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
    raise RuntimeError("Spotify credentials not found in environment.")