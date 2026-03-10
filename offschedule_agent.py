"""Off-Schedule Discovery Agent for SXSW 2026

Scrapes unofficial sources (Do512, Austin Chronicle, Reddit, brand popups)
for events not in the official SXSW schedule, then deep-merges them into data.json
after 3-point deduplication against official events.
"""

import hashlib
import json
import os
import re
import time
from datetime import datetime
from difflib import SequenceMatcher

import requests
from geopy.geocoders import Nominatim

DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")
OFFSCHEDULE_CACHE = os.path.join(os.path.dirname(__file__), "offschedule_cache.json")

geocoder = Nominatim(user_agent="sxsw-offschedule-2026")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# SXSW 2026 date range for relevance filtering
SXSW_START = "2026-03-07"
SXSW_END = "2026-03-22"


def _generate_id(name, venue):
    """Generate deterministic SHA-256 ID from event name + venue."""
    raw = f"{name.strip().lower()}|{venue.strip().lower()}"
    return "OFF_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _similarity(a, b):
    """Fuzzy string similarity (0.0 - 1.0) using SequenceMatcher."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return {"events": [], "genres": [], "venues": {}, "last_scraped": None}


def _save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _load_cache():
    if os.path.exists(OFFSCHEDULE_CACHE):
        with open(OFFSCHEDULE_CACHE) as f:
            return json.load(f)
    return {"discovered": [], "last_run": None, "sources_checked": []}


def _save_cache(cache):
    with open(OFFSCHEDULE_CACHE, "w") as f:
        json.dump(cache, f, indent=2)


def _geocode_address(address):
    """Geocode a raw address string, appending Austin TX if needed."""
    if not address:
        return None, None
    try:
        query = address
        if "austin" not in address.lower():
            query = f"{address}, Austin, TX"
        location = geocoder.geocode(query)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"    Geocode error for '{address}': {e}")
    return None, None


def _parse_age_policy(text):
    """Extract age policy from text."""
    text_lower = text.lower() if text else ""
    if "21+" in text_lower or "21 and over" in text_lower:
        return "21+"
    if "18+" in text_lower or "18 and over" in text_lower:
        return "18+"
    if "all ages" in text_lower:
        return "All Ages"
    return "Unknown"


def _parse_datetime(date_str, time_str=None):
    """Attempt to parse various date/time formats into ISO 8601."""
    if not date_str:
        return None

    # Try common formats
    formats = [
        "%Y-%m-%d %I:%M%p",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%m/%d/%Y %I:%M%p",
        "%B %d, %Y %I:%M%p",
        "%b %d, %Y %I:%M%p",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]

    combined = f"{date_str} {time_str}".strip() if time_str else date_str

    for fmt in formats:
        try:
            dt = datetime.strptime(combined.strip(), fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None


# ─── Source Scrapers ──────────────────────────────────────────────────

def scrape_do512():
    """Scrape Do512.com for unofficial SXSW events."""
    events = []
    print("  [Do512] Searching for SXSW 2026 events...")

    search_urls = [
        "https://do512.com/events/search?q=sxsw+2026",
        "https://do512.com/events/search?q=sxsw+unofficial+2026",
        "https://do512.com/events/search?q=sxsw+party+2026",
        "https://do512.com/events/search?q=sxsw+free+2026",
        "https://do512.com/events/search?q=sxsw+day+party+2026",
    ]

    for url in search_urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if not resp.ok:
                continue

            html = resp.text

            # Parse event blocks from Do512 search results
            # Do512 uses JSON-LD or structured data
            ld_blocks = re.findall(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                html, re.DOTALL
            )
            for block in ld_blocks:
                try:
                    ld = json.loads(block)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") != "Event":
                            continue
                        name = item.get("name", "")
                        if not name:
                            continue

                        venue_name = ""
                        raw_address = ""
                        if isinstance(item.get("location"), dict):
                            loc = item["location"]
                            venue_name = loc.get("name", "")
                            addr = loc.get("address", {})
                            if isinstance(addr, dict):
                                raw_address = addr.get("streetAddress", "")
                            elif isinstance(addr, str):
                                raw_address = addr

                        start = item.get("startDate", "")
                        end = item.get("endDate", "")
                        desc = item.get("description", "")
                        rsvp = item.get("url", "")

                        events.append({
                            "name": name,
                            "description": desc[:500] if desc else "",
                            "venue_name": venue_name,
                            "raw_address": raw_address,
                            "start_iso": start,
                            "end_iso": end,
                            "rsvp_url": rsvp,
                            "age_text": "",
                            "source": "Do512",
                            "confidence": 0.75,
                        })
                except (json.JSONDecodeError, KeyError):
                    continue

            # Also try to find event cards in HTML
            cards = re.findall(
                r'class="[^"]*event[_-]?card[^"]*"[^>]*>(.*?)</(?:div|article)>',
                html, re.DOTALL | re.IGNORECASE
            )
            for card in cards:
                title_match = re.search(
                    r'class="[^"]*(?:title|name)[^"]*"[^>]*>([^<]+)', card
                )
                venue_match = re.search(
                    r'class="[^"]*venue[^"]*"[^>]*>([^<]+)', card
                )
                time_match = re.search(
                    r'class="[^"]*(?:date|time)[^"]*"[^>]*>([^<]+)', card
                )

                if title_match:
                    name = title_match.group(1).strip()
                    # Skip if clearly not SXSW related
                    if not any(
                        kw in name.lower()
                        for kw in ["sxsw", "south by", "unofficial", "party", "popup"]
                    ) and not any(
                        kw in card.lower()
                        for kw in ["sxsw", "south by"]
                    ):
                        continue

                    events.append({
                        "name": name,
                        "description": "",
                        "venue_name": venue_match.group(1).strip() if venue_match else "",
                        "raw_address": "",
                        "start_iso": "",
                        "end_iso": "",
                        "rsvp_url": "",
                        "age_text": "",
                        "source": "Do512",
                        "confidence": 0.5,
                    })

            time.sleep(1)  # Be polite
        except Exception as e:
            print(f"    [Do512] Error fetching {url}: {e}")

    # Deduplicate within source
    seen = set()
    unique = []
    for ev in events:
        key = ev["name"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(ev)

    print(f"  [Do512] Found {len(unique)} candidate events")
    return unique


def scrape_austin_chronicle():
    """Scrape Austin Chronicle for unofficial SXSW events."""
    events = []
    print("  [Austin Chronicle] Searching for SXSW 2026 events...")

    urls = [
        "https://www.austinchronicle.com/search/?q=sxsw+2026+unofficial",
        "https://www.austinchronicle.com/search/?q=sxsw+2026+free+party",
        "https://www.austinchronicle.com/search/?q=sxsw+2026+day+party",
    ]

    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if not resp.ok:
                continue

            html = resp.text

            # Parse JSON-LD
            ld_blocks = re.findall(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                html, re.DOTALL
            )
            for block in ld_blocks:
                try:
                    ld = json.loads(block)
                    items = ld if isinstance(ld, list) else [ld]
                    for item in items:
                        if item.get("@type") != "Event":
                            continue
                        name = item.get("name", "")
                        if not name:
                            continue

                        venue_name = ""
                        raw_address = ""
                        if isinstance(item.get("location"), dict):
                            loc = item["location"]
                            venue_name = loc.get("name", "")
                            addr = loc.get("address", {})
                            if isinstance(addr, dict):
                                raw_address = addr.get("streetAddress", "")
                            elif isinstance(addr, str):
                                raw_address = addr

                        events.append({
                            "name": name,
                            "description": item.get("description", "")[:500],
                            "venue_name": venue_name,
                            "raw_address": raw_address,
                            "start_iso": item.get("startDate", ""),
                            "end_iso": item.get("endDate", ""),
                            "rsvp_url": item.get("url", ""),
                            "age_text": "",
                            "source": "Austin Chronicle",
                            "confidence": 0.7,
                        })
                except (json.JSONDecodeError, KeyError):
                    continue

            # Parse event listing links
            event_links = re.findall(
                r'href="(/events/[^"]*sxsw[^"]*)"', html, re.IGNORECASE
            )
            for link in event_links[:20]:  # Cap at 20
                try:
                    full_url = f"https://www.austinchronicle.com{link}"
                    eresp = requests.get(full_url, headers=HEADERS, timeout=10)
                    if not eresp.ok:
                        continue

                    ehtml = eresp.text
                    title_match = re.search(r"<h1[^>]*>([^<]+)</h1>", ehtml)
                    if not title_match:
                        continue

                    name = title_match.group(1).strip()
                    venue_match = re.search(
                        r'class="[^"]*venue[^"]*"[^>]*>([^<]+)', ehtml
                    )
                    desc_match = re.search(
                        r'<meta\s+name="description"\s+content="([^"]*)"', ehtml
                    )

                    events.append({
                        "name": name,
                        "description": desc_match.group(1)[:500] if desc_match else "",
                        "venue_name": venue_match.group(1).strip() if venue_match else "",
                        "raw_address": "",
                        "start_iso": "",
                        "end_iso": "",
                        "rsvp_url": full_url,
                        "age_text": "",
                        "source": "Austin Chronicle",
                        "confidence": 0.65,
                    })
                    time.sleep(0.5)
                except Exception:
                    continue

            time.sleep(1)
        except Exception as e:
            print(f"    [Austin Chronicle] Error: {e}")

    seen = set()
    unique = []
    for ev in events:
        key = ev["name"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(ev)

    print(f"  [Austin Chronicle] Found {len(unique)} candidate events")
    return unique


def scrape_reddit():
    """Search Reddit for unofficial SXSW 2026 event threads."""
    events = []
    print("  [Reddit] Searching for SXSW 2026 event threads...")

    # Use Reddit JSON API (no auth needed for public data)
    search_queries = [
        "sxsw 2026 unofficial events",
        "sxsw 2026 rsvp list",
        "sxsw 2026 free parties",
        "sxsw 2026 day party",
    ]

    subreddits = ["sxsw", "Austin", "TexasMusic"]

    for sub in subreddits:
        for query in search_queries:
            try:
                url = f"https://www.reddit.com/r/{sub}/search.json"
                params = {
                    "q": query,
                    "restrict_sr": "on",
                    "sort": "new",
                    "limit": 10,
                    "t": "year",
                }
                resp = requests.get(
                    url, params=params,
                    headers={**HEADERS, "Accept": "application/json"},
                    timeout=10,
                )
                if not resp.ok:
                    continue

                data = resp.json()
                posts = data.get("data", {}).get("children", [])

                for post in posts:
                    pd = post.get("data", {})
                    title = pd.get("title", "")
                    body = pd.get("selftext", "")
                    post_url = f"https://reddit.com{pd.get('permalink', '')}"

                    # Look for event-like patterns in the body
                    # Common patterns: "Event Name @ Venue - Date Time"
                    # or "- Event Name at Venue (FREE RSVP)"
                    event_patterns = re.findall(
                        r'[-*]\s*(.+?)\s*[@at]+\s*(.+?)(?:\s*[-|]\s*(.+?))?(?:\s*\(([^)]*)\))?$',
                        body, re.MULTILINE | re.IGNORECASE
                    )

                    for match in event_patterns:
                        name = match[0].strip()
                        venue = match[1].strip() if len(match) > 1 else ""
                        time_str = match[2].strip() if len(match) > 2 else ""
                        extra = match[3].strip() if len(match) > 3 else ""

                        # Skip very short or clearly non-event lines
                        if len(name) < 3 or name.lower().startswith(("http", "edit", "note")):
                            continue

                        rsvp_match = re.search(r'https?://\S+', extra + " " + name)
                        rsvp_url = rsvp_match.group(0) if rsvp_match else post_url

                        events.append({
                            "name": re.sub(r'\*+', '', name).strip(),
                            "description": f"Found in Reddit r/{sub} thread: {title[:200]}",
                            "venue_name": re.sub(r'\*+', '', venue).strip(),
                            "raw_address": "",
                            "start_iso": "",
                            "end_iso": "",
                            "rsvp_url": rsvp_url,
                            "age_text": extra,
                            "source": f"Reddit r/{sub}",
                            "confidence": 0.45,
                        })

                    # Also check for RSVP list links in body
                    rsvp_links = re.findall(
                        r'(https?://(?:www\.)?(?:eventbrite|rsvp|partiful|lu\.ma|splash|do512)\S+)',
                        body
                    )
                    for link in rsvp_links[:5]:
                        events.append({
                            "name": f"RSVP Event from r/{sub}",
                            "description": f"RSVP link found in: {title[:200]}",
                            "venue_name": "",
                            "raw_address": "",
                            "start_iso": "",
                            "end_iso": "",
                            "rsvp_url": link,
                            "age_text": "",
                            "source": f"Reddit r/{sub}",
                            "confidence": 0.3,
                        })

                time.sleep(2)  # Reddit rate limit
            except Exception as e:
                print(f"    [Reddit] Error searching r/{sub}: {e}")

    seen = set()
    unique = []
    for ev in events:
        key = (ev["name"].lower().strip(), ev["venue_name"].lower().strip())
        if key not in seen:
            seen.add(key)
            unique.append(ev)

    print(f"  [Reddit] Found {len(unique)} candidate events")
    return unique


def scrape_brand_popups():
    """Search for brand-sponsored SXSW popup announcements."""
    events = []
    print("  [Brand Popups] Searching for brand activations...")

    # Known brand activation patterns for SXSW
    brand_searches = [
        ("Amazon", "amazon sxsw 2026 popup austin"),
        ("Spotify", "spotify sxsw 2026 house austin"),
        ("Delta", "delta sxsw 2026 experience austin"),
        ("Rolling Stone", "rolling stone sxsw 2026 party"),
        ("Fader Fort", "fader fort sxsw 2026"),
        ("Pitchfork", "pitchfork sxsw 2026 party"),
        ("NPR", "npr music sxsw 2026 showcase"),
        ("Paste", "paste sxsw 2026 party"),
        ("Brooklyn Vegan", "brooklyn vegan sxsw 2026"),
        ("Consequence", "consequence of sound sxsw 2026"),
    ]

    for brand, query in brand_searches:
        try:
            # Try Google via a simple search scrape
            url = "https://www.google.com/search"
            params = {"q": query, "num": 5}
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if not resp.ok:
                continue

            html = resp.text

            # Extract search result snippets
            snippets = re.findall(
                r'<div[^>]*class="[^"]*(?:VwiC3b|s3v9rd|IsZvec)[^"]*"[^>]*>(.*?)</div>',
                html, re.DOTALL
            )

            for snippet in snippets:
                clean = re.sub(r'<[^>]+>', '', snippet).strip()
                if not clean or len(clean) < 20:
                    continue

                # Look for event details in snippet
                date_match = re.search(
                    r'(March\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s*2026)', clean
                )
                venue_match = re.search(
                    r'(?:at|@)\s+([A-Z][^,.]{3,40})', clean
                )

                if date_match or "sxsw" in clean.lower():
                    events.append({
                        "name": f"{brand} SXSW 2026 Experience",
                        "description": clean[:500],
                        "venue_name": venue_match.group(1).strip() if venue_match else "",
                        "raw_address": "",
                        "start_iso": "",
                        "end_iso": "",
                        "rsvp_url": "",
                        "age_text": "",
                        "source": f"Brand: {brand}",
                        "confidence": 0.4,
                    })

            time.sleep(2)  # Don't hammer Google
        except Exception as e:
            print(f"    [Brand Popups] Error searching {brand}: {e}")

    seen = set()
    unique = []
    for ev in events:
        key = ev["name"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(ev)

    print(f"  [Brand Popups] Found {len(unique)} candidate events")
    return unique


# ─── Deduplication Engine ─────────────────────────────────────────────

def is_duplicate(candidate, official_events, venue_map):
    """3-point collision check against official data.

    Returns (is_dup, reason) tuple.
    """
    cand_name = candidate["name"].lower().strip()
    cand_venue = candidate["venue_name"].lower().strip()
    cand_start = candidate.get("start_iso", "")

    for official in official_events:
        off_name = official.get("name", "").lower().strip()
        off_venue = official.get("venue", "").lower().strip()
        off_start = official.get("start_iso", "")
        off_date = official.get("date", "")

        # 1. Fuzzy Name Match (threshold > 0.85)
        name_sim = _similarity(cand_name, off_name)
        if name_sim > 0.85:
            return True, f"Name match ({name_sim:.2f}): '{official.get('name')}'"

        # 2. Venue/Address Match + similar name (lower threshold)
        venue_sim = _similarity(cand_venue, off_venue)
        if venue_sim > 0.8 and name_sim > 0.5:
            return True, f"Venue+name match: '{off_venue}' + '{off_name}'"

        # Check address match
        cand_addr = candidate.get("raw_address", "").lower()
        off_addr = official.get("venue_address", "").lower()
        if cand_addr and off_addr and _similarity(cand_addr, off_addr) > 0.8:
            if name_sim > 0.5:
                return True, f"Address+name match: '{off_addr}'"

        # 3. Time Window Collision (same venue, same 24h window)
        if venue_sim > 0.7:
            # Check if same date/24h window
            cand_date = ""
            if cand_start:
                try:
                    cand_date = cand_start[:10]
                except Exception:
                    pass

            if cand_date and cand_date == off_date:
                return True, f"Same venue+date: '{off_venue}' on {off_date}"

    # Also check against venue names in venue_map
    for vid, vinfo in venue_map.items():
        v_name = vinfo.get("name", "").lower()
        v_addr = vinfo.get("address", "").lower()
        if cand_venue and _similarity(cand_venue, v_name) > 0.85:
            # Venue exists - not a dup by itself, but flag if name is close
            pass

    return False, ""


# ─── Main Discovery Pipeline ─────────────────────────────────────────

def discover_events():
    """Main discovery function. Scrapes sources, deduplicates, and merges."""
    print("=" * 60)
    print("Off-Schedule Discovery Agent - SXSW 2026")
    print("=" * 60)

    # Load official data
    data = _load_data()
    official_events = data.get("events", [])
    venue_map = data.get("venues", {})
    print(f"\nLoaded {len(official_events)} official events for dedup check")

    # Scrape all sources
    print("\n--- Scraping sources ---")
    candidates = []

    sources = [
        ("Do512", scrape_do512),
        ("Austin Chronicle", scrape_austin_chronicle),
        ("Reddit", scrape_reddit),
        ("Brand Popups", scrape_brand_popups),
    ]

    for source_name, scraper_fn in sources:
        try:
            results = scraper_fn()
            candidates.extend(results)
        except Exception as e:
            print(f"  [{source_name}] FATAL ERROR: {e}")

    print(f"\nTotal raw candidates: {len(candidates)}")

    # Filter: minimum confidence
    candidates = [c for c in candidates if c.get("confidence", 0) >= 0.3]
    print(f"After confidence filter (>=0.3): {len(candidates)}")

    # Filter: must have a name longer than 5 chars
    candidates = [c for c in candidates if len(c.get("name", "")) > 5]
    print(f"After name length filter: {len(candidates)}")

    # Cross-source deduplication
    seen_names = set()
    unique_candidates = []
    for c in candidates:
        key = c["name"].lower().strip()
        if key not in seen_names:
            seen_names.add(key)
            unique_candidates.append(c)
    candidates = unique_candidates
    print(f"After cross-source dedup: {len(candidates)}")

    # 3-point deduplication against official data
    print("\n--- Deduplication against official data ---")
    new_events = []
    dupes_found = 0
    for c in candidates:
        is_dup, reason = is_duplicate(c, official_events, venue_map)
        if is_dup:
            dupes_found += 1
            print(f"  DUPE: '{c['name']}' - {reason}")
        else:
            new_events.append(c)

    print(f"\nDuplicates discarded: {dupes_found}")
    print(f"New events to add: {len(new_events)}")

    # Geocode and convert to final schema
    print("\n--- Geocoding and finalizing ---")
    final_events = []
    for i, ev in enumerate(new_events):
        print(f"  [{i+1}/{len(new_events)}] Processing: {ev['name']}")

        # Geocode venue
        lat, lng = None, None
        address = ev.get("raw_address", "") or ev.get("venue_name", "")
        if address:
            lat, lng = _geocode_address(address)
            if lat:
                print(f"    Geocoded: {lat}, {lng}")
            time.sleep(1)  # Nominatim rate limit

        # Parse dates
        start_iso = ev.get("start_iso", "")
        end_iso = ev.get("end_iso", "")

        # Extract date from ISO
        event_date = ""
        if start_iso:
            try:
                event_date = start_iso[:10]
            except Exception:
                pass

        # Determine age policy
        age_policy = _parse_age_policy(ev.get("age_text", ""))

        event_id = _generate_id(ev["name"], ev.get("venue_name", ""))

        # Parse display times
        start_time = ""
        end_time = ""
        if start_iso:
            try:
                dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                from datetime import timedelta
                dt_ct = dt - timedelta(hours=5)
                start_time = dt_ct.strftime("%-I:%M%p")
                event_date = event_date or dt_ct.strftime("%Y-%m-%d")
            except Exception:
                start_time = start_iso
        if end_iso:
            try:
                dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                from datetime import timedelta
                dt_ct = dt - timedelta(hours=5)
                end_time = dt_ct.strftime("%-I:%M%p")
            except Exception:
                end_time = end_iso

        final = {
            "id": event_id,
            "name": ev["name"],
            "description": ev.get("description", ""),
            "is_official": False,
            "date": event_date,
            "start_time": start_time,
            "end_time": end_time,
            "start_iso": start_iso,
            "end_iso": end_iso,
            "venue": ev.get("venue_name", ""),
            "venue_id": "",
            "venue_address": ev.get("raw_address", ""),
            "venue_lat": lat,
            "venue_lng": lng,
            "access_levels": ["Free"],
            "genre": "",
            "event_type": "Unofficial",
            "event_category": "unofficial",
            "thumbnail_url": "",
            "age_policy": age_policy,
            "sponsor": "",
            "metadata": {
                "rsvp_url": ev.get("rsvp_url", ""),
                "confidence_score": ev.get("confidence", 0),
                "source": ev.get("source", ""),
                "discovered_at": datetime.utcnow().isoformat(),
            },
        }
        final_events.append(final)

    # Deep-merge into data.json
    print(f"\n--- Merging {len(final_events)} events into data.json ---")

    # Remove old unofficial events first (to allow re-runs)
    existing_official = [e for e in data["events"] if e.get("is_official", True)]
    data["events"] = existing_official + final_events

    # Update genres list
    all_genres = sorted(set(e.get("genre", "") for e in data["events"] if e.get("genre")))
    data["genres"] = all_genres

    # Save
    _save_data(data)

    # Save cache
    cache = _load_cache()
    cache["discovered"] = [
        {"id": e["id"], "name": e["name"], "source": e["metadata"]["source"]}
        for e in final_events
    ]
    cache["last_run"] = datetime.utcnow().isoformat()
    cache["sources_checked"] = ["Do512", "Austin Chronicle", "Reddit", "Brand Popups"]
    _save_cache(cache)

    print(f"\nDone! Merged {len(final_events)} unofficial events.")
    print(f"Total events in data.json: {len(data['events'])}")
    print(f"  Official: {len(existing_official)}")
    print(f"  Unofficial: {len(final_events)}")

    return final_events


if __name__ == "__main__":
    discover_events()
