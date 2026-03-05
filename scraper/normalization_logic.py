### 2.24.26 Defining normailization

import re
import unicodedata
import html
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple


# ----------------------------
# Helpers
# ----------------------------

def strip_diacritics(s: str) -> str:
    """Convert to NFKD and drop combining marks: señorita -> senorita"""
    if s is None:
        return s
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def squash_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_common_punct(s: str) -> str:
    """
    Normalize separators before stripping punctuation:
    - Convert & -> and
    - Convert slashes to spaces
    """
    if s is None:
        return s
    s = s.replace("&", " and ")
    s = s.replace("/", " ")
    return s

def normalize_for_key(s: str) -> str:
    """
    Better punctuation handling:
    - Remove apostrophes and periods entirely
    - Convert &, /, -, _ to spaces
    - Everything else non-alnum -> space
    """
    if not s:
        return s

    # remove apostrophes and periods completely
    s = s.replace("'", "")
    s = s.replace(".", "")

    # normalize separators
    s = s.replace("&", " and ")
    s = s.replace("/", " ")
    s = s.replace("-", " ")
    s = s.replace("_", " ")

    # remove remaining non-alphanumeric
    s = re.sub(r"[^0-9a-z]+", " ", s)

    return squash_spaces(s)
def drop_leading_the(s: str) -> str:
    return re.sub(r"^the\s+", "", s)

def extract_year(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return int(m.group(1)) if m else None


# ----------------------------
# Feature/Version detection
# ----------------------------

VERSION_KEYWORDS = {
    "live": ["live", "at the", "at ", "tour", "concert", "theatre", "theater", "stadium", "arena"],
    "remaster": ["remaster", "remastered"],
    "remix": ["remix", "mix"],
    "edit": ["radio edit", "edit"],
    "instrumental": ["instrumental"],
    "acoustic": ["acoustic"],
    "demo": ["demo"],
    "explicit": ["explicit"],
    "clean": ["clean"],
    "feat": ["feat", "ft", "featuring"],
}

def classify_version_type(version_note_raw: str) -> str:
    """
    Very lightweight classifier based on keywords.
    Returns one of: live, remaster, remix, edit, instrumental, acoustic, demo, explicit, clean, feat, other
    """
    if not version_note_raw:
        return "other"
    v = version_note_raw.lower()

    # More specific first
    if "radio edit" in v:
        return "edit"
    if "remaster" in v:
        return "remaster"
    if "remix" in v:
        return "remix"

    # Keyword buckets
    for k, kws in VERSION_KEYWORDS.items():
        for kw in kws:
            if kw in v:
                return k
    return "other"


# ----------------------------
# Feat extraction
# ----------------------------

# Covers: "feat X", "feat. X", "ft X", "ft. X", "featuring X"
FEAT_REGEX = re.compile(r"\b(feat\.?|ft\.?|featuring)\b\s*(.+)$", flags=re.IGNORECASE)

def extract_feat_artists_from_text(s: str) -> Tuple[str, Optional[str]]:
    """
    If s contains a trailing feat marker, extract featured artists and return (cleaned_without_feat, feat_artists_raw).
    Otherwise returns (s, None).
    """
    if not s:
        return s, None
    m = FEAT_REGEX.search(s)
    if not m:
        return s, None
    feat_raw = m.group(2).strip()
    # Remove the feat part from the string
    cleaned = s[:m.start()].strip()
    return cleaned, feat_raw


# ----------------------------
# Title parenthetical + suffix extraction
# ----------------------------

TRAILING_PARENS_REGEX = re.compile(r"(?:\s*\(([^()]*)\)\s*)+$")

def extract_trailing_parentheticals(title: str) -> Tuple[str, Optional[str]]:
    """
    Extract ALL trailing (...) groups (combined) from the end of the title.
    Returns (title_without_trailing_parens, combined_note) where combined_note joins groups with ' | '.
    Example:
      "Song (live) (2001)" -> ("Song", "live | 2001")
    """
    if not title:
        return title, None

    m = TRAILING_PARENS_REGEX.search(title)
    if not m:
        return title, None

    # Find all trailing parentheses groups
    trailing = title[m.start():].strip()
    groups = re.findall(r"\(([^()]*)\)", trailing)
    groups = [g.strip() for g in groups if g.strip()]

    title_wo = title[:m.start()].strip()
    combined = " | ".join(groups) if groups else None
    return title_wo, combined


# Also treat suffixes like " - Radio Edit" or ": Live" as version notes when keyword present
SUFFIX_SPLIT_REGEX = re.compile(r"\s*[-–—:]\s*")

def extract_version_suffix(title: str) -> Tuple[str, Optional[str]]:
    """
    If title ends with a suffix separated by dash/colon, and the suffix contains version keywords,
    extract it as version_note.
    Example: "Hello - Radio Edit" -> ("Hello", "Radio Edit")
    """
    if not title:
        return title, None

    parts = SUFFIX_SPLIT_REGEX.split(title)
    if len(parts) < 2:
        return title, None

    # Consider only LAST suffix chunk
    base = " - ".join(parts[:-1]).strip()
    suffix = parts[-1].strip()

    # If suffix contains a keyword we care about, treat as version note
    suffix_lc = suffix.lower()
    if any(kw in suffix_lc for kws in VERSION_KEYWORDS.values() for kw in kws):
        return base, suffix
    return title, None


# ----------------------------
# Normalization core functions
# ----------------------------

def normalize_artist(artist: Optional[str]) -> Optional[str]:
    if artist is None:
        return None
    s = artist.strip().lower()
    s = normalize_common_punct(s)
    s = strip_diacritics(s)

    # Remove punctuation -> spaces, keep alnum + spaces
    s = normalize_for_key(s)

    # Optional: drop leading "the"
    s = drop_leading_the(s)

    return s or None

def normalize_title(title: Optional[str]) -> Dict[str, Any]:
    """
    Returns dict with:
      norm_title_core
      norm_title_full
      version_note_raw
      version_note
      version_type
      version_year
      feat_artists_raw
      feat_artists
      title_base_raw
    """
    out = {
        "norm_title_core": None,
        "norm_title_full": None,
        "version_note_raw": None,
        "version_note": None,
        "version_type": None,
        "version_year": None,
        "feat_artists_raw": None,
        "feat_artists": None,
        "title_base_raw": None,
    }
    if title is None:
        return out

    t0 = title.strip()
    if not t0:
        return out

    # Step 1: extract trailing parentheticals
    title_wo_parens, paren_note = extract_trailing_parentheticals(t0)

    # Step 2: extract version suffix like " - Radio Edit" (only if no parens extracted)
    suffix_note = None
    if not paren_note:
        title_wo_suffix, suffix_note = extract_version_suffix(title_wo_parens)
    else:
        title_wo_suffix = title_wo_parens

    # Determine version_note_raw (combine if both exist; usually one)
    notes = []
    if paren_note:
        notes.append(paren_note)
    if suffix_note:
        notes.append(suffix_note)
    version_note_raw = " | ".join(notes) if notes else None

    # Step 3: extract feat from version note if it contains feat (rare) OR from title itself
    # First, try extracting feat from the title WITHOUT parens/suffix (common "Song feat X")
    base_for_feat = title_wo_suffix
    base_wo_feat, feat_raw = extract_feat_artists_from_text(base_for_feat)

    # Also try extracting feat from version_note_raw if still none (e.g., "(feat X)")
    if not feat_raw and version_note_raw:
        vn_wo_feat, feat_raw2 = extract_feat_artists_from_text(version_note_raw)
        if feat_raw2:
            feat_raw = feat_raw2
            version_note_raw = vn_wo_feat  # remove feat part from version note

    # Base title used for core
    title_base_raw = base_wo_feat.strip()

    # Step 4: classify version + parse year
    version_type = classify_version_type(version_note_raw) if version_note_raw else None
    version_year = extract_year(version_note_raw) if version_note_raw else None

    # Step 5: Build FULL title string = original base + version_note_raw + feat_raw (if you want full to keep it)
    # We include version note and feat in full so it remains searchable/disambiguating.
    full_components = [title_base_raw]
    if version_note_raw:
        full_components.append(version_note_raw)
    if feat_raw:
        full_components.append("feat " + feat_raw)
    full_raw = " ".join([c for c in full_components if c]).strip()

    # Step 6: Normalize core title
    core = title_base_raw.lower()
    core = normalize_common_punct(core)
    core = strip_diacritics(core)
    core = normalize_for_key(core)

    # Step 7: Normalize full title
    full = full_raw.lower()
    full = normalize_common_punct(full)
    full = strip_diacritics(full)
    full = normalize_for_key(full)

    # Step 8: Normalize feat artists
    feat_norm = None
    if feat_raw:
        fa = feat_raw.lower()
        fa = normalize_common_punct(fa)
        fa = strip_diacritics(fa)
        fa = normalize_for_key(fa)
        feat_norm = fa or None

    # Step 9: Normalize version note (separately)
    version_note_norm = None
    if version_note_raw:
        vn = version_note_raw.lower()
        vn = normalize_common_punct(vn)
        vn = strip_diacritics(vn)
        vn = normalize_for_key(vn)
        version_note_norm = vn or None

    out.update({
        "norm_title_core": core or None,
        "norm_title_full": full or None,
        "version_note_raw": version_note_raw,
        "version_note": version_note_norm,
        "version_type": version_type,
        "version_year": version_year,
        "feat_artists_raw": feat_raw,
        "feat_artists": feat_norm,
        "title_base_raw": title_base_raw,
    })
    return out

def normalize_title_artist(title: Optional[str], artist: Optional[str]) -> Dict[str, Any]:
    """
    High-level wrapper that returns all computed normalization fields + keys.
    """
    t = normalize_title(title)
    a_norm = normalize_artist(artist)

    norm_key_core = None
    norm_key_full = None
    if a_norm and t.get("norm_title_core"):
        norm_key_core = f"{a_norm} - {t['norm_title_core']}"
    if a_norm and t.get("norm_title_full"):
        norm_key_full = f"{a_norm} - {t['norm_title_full']}"

    return {
        "norm_artist": a_norm,
        **t,
        "norm_key_core": norm_key_core,
        "norm_key_full": norm_key_full,
    }