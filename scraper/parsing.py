from bs4 import BeautifulSoup
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs


def extract_hour_from_source(source_url):
    try:
        qs = parse_qs(urlparse(source_url).query)
        if 'hour' in qs:
            return int(qs['hour'][0])
    except Exception:
        pass
    return None


def parse_timestamp_guess(time_text, source_url):
    if not time_text:
        return None

    time_text_norm = time_text.strip().replace(".", "").upper()

    date_part = None
    try:
        parsed = urlparse(source_url)
        qs = parse_qs(parsed.query)
        date_part = qs.get("date", [None])[0]
    except Exception:
        date_part = None

    m = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)(?:\s*(AM|PM))?$", time_text_norm)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        ampm = m.group(3)

        if ampm:
            if ampm == "PM" and hh != 12:
                hh += 12
            if ampm == "AM" and hh == 12:
                hh = 0

        if date_part:
            try:
                dt_date = datetime.strptime(date_part, "%Y-%m-%d").date()
            except Exception:
                dt_date = datetime.today().date()
        else:
            dt_date = datetime.today().date()

        return datetime(dt_date.year, dt_date.month, dt_date.day, hh, mm)

    return None

import re
from datetime import datetime as _dt
from urllib.parse import urlparse, parse_qs


def parse_station_show_from_header(soup, source_url=None):
    headers = []

    for el in soup.select("h1, h2, h3"):
        txt = el.get_text(" ", strip=True)
        headers.append(txt)

    # Try to match header containing show in parentheses
    for txt in reversed(headers):
        m = re.search(r"\(([^)]+)\)\s*$", txt)
        if m:
            return m.group(1).strip()

    return None


def parse_played_page(html_text, source_url):
    soup = BeautifulSoup(html_text, "lxml")
    plays = []

    ul = soup.select_one("ul.gm-sec.divide-rows") or soup.select_one("ul.gm-sec")
    if not ul:
        return []

    station_show = parse_station_show_from_header(soup, source_url)

    for li in ul.select("li"):
        try:
            time_node = li.select_one("p.gm-sec-meta span")
            time_text = time_node.get_text(strip=True) if time_node else None

            title_node = li.select_one("p.gm-sec-title[data-trackid]") or li.select_one("p.gm-sec-title")
            raw_title = title_node.get_text(strip=True) if title_node else None

            artist_node = li.select_one("p.gm-sec-title a")
            if not artist_node:
                gm_titles = li.select("p.gm-sec-title")
                if len(gm_titles) >= 2:
                    artist_node = gm_titles[1]
            raw_artist = artist_node.get_text(strip=True) if artist_node else None

            dt = parse_timestamp_guess(time_text, source_url)

            if not raw_title or not raw_artist or not dt:
                continue

            plays.append({
                "play_ts": dt.isoformat(),
                "station_show": station_show,
                "title": raw_title.strip(),
                "artist": raw_artist.strip(),
                "raw_title": raw_title,
                "raw_artist": raw_artist,
                "raw_time_text": time_text,
                "confidence": "parsed",
                "is_music_show": station_show != "GPS for Your Finances with Ken Mahoney",
                "source_url": source_url,
                "scraped_at": datetime.utcnow().isoformat()
            })

        except Exception:
            continue

    return plays