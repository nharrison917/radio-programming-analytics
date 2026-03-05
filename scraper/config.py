from dotenv import load_dotenv
import os

load_dotenv()

# --- Monitoring thresholds ---

MIN_PLAYS_PER_HOUR = 3
MAX_PLAYS_PER_HOUR = 30
FLAG_NULL_STATION_SHOW = True
FLAG_SUSPICIOUS_TITLE = True

# -------------------

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

BASE_PLAYED_URL = "https://www.1071thepeak.com/played/"

HEADERS = {
    "User-Agent": "PeakRadioScraper/1.0 (Academic Research; contact: nharrison1@gmail.com)"
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