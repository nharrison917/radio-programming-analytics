from dotenv import load_dotenv
import os

load_dotenv()

# --- Environment Variables ---

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SCRAPER_CONTACT = os.getenv("SCRAPER_CONTACT")

if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
    raise RuntimeError("Spotify credentials not found in environment.")

# --- Database ---

DB_PATH = "radio_plays.db"

# --- Scraping: 107.1 The Peak Website ---

BASE_PLAYED_URL = "https://www.1071thepeak.com/played/"

HEADERS = {
    "User-Agent": f"PeakRadioScraper/1.0 (Academic Research; contact: {SCRAPER_CONTACT})"
}

CRAWL_DELAY = 8          # Seconds between page requests (respect robots.txt)
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF = 5

DISALLOWED_PATH_FRAGMENTS = [
    "/artist/",
    "/searchresults/",
    "/_hp/"
]

# --- Monitoring / Audit ---

MIN_PLAYS_PER_HOUR = 3
MAX_PLAYS_PER_HOUR = 30
FLAG_NULL_STATION_SHOW = True
FLAG_SUSPICIOUS_TITLE = True

# Shows known to play little or no music - suppress low play count warnings
LOW_PLAY_SUPPRESSED_SHOWS = {
    "GPS for Your Finances with Ken Mahoney",
}

# If a play title contains any of these strings, suppress low play count
# warnings for that hour AND the following hour
LOW_PLAY_SUPPRESSED_TITLE_SIGNALS = [
    "Anything, Anything with Rich Russo",
]

# --- Spotify Artist Enrichment ---

ARTIST_ENRICHMENT_BATCH_SIZE = 45   # Artists to attempt per weekly run
ARTIST_ENRICHMENT_CHUNK_SIZE = 3    # Artists per chunk before cooldown
ARTIST_ENRICHMENT_COOLDOWN_SECONDS = 25  # Pause between chunks (seconds)
ARTIST_ENRICHMENT_REQUEST_DELAY = 3      # Pause between API page requests (seconds)
