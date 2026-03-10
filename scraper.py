"""SXSW 2026 Music Showcase Scraper"""
import json
import time
import requests
from datetime import datetime

BASE_URL = "https://schedule.sxsw.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/2026/search/event",
}


def get_session():
    """Get session cookie and CSRF token."""
    session = requests.Session()
    resp = session.get(f"{BASE_URL}/2026/search/event", headers={
        "User-Agent": HEADERS["User-Agent"]
    })
    resp.raise_for_status()
    # Extract CSRF token from meta tag
    import re
    match = re.search(r'csrf-token"\s+content="([^"]+)"', resp.text)
    if not match:
        raise Exception("Could not find CSRF token")
    csrf_token = match.group(1)
    return session, csrf_token


def scrape_events(session, csrf_token):
    """Scrape all Music Showcase events."""
    headers = {**HEADERS, "X-CSRF-Token": csrf_token}
    payload = {
        "term": "",
        "filters": [
            {"models": ["event"], "field": "credential_types", "value": "music"},
            {"models": ["event"], "field": "event_type", "value": "Showcase"},
        ],
        "models": ["event"],
        "page": 1,
    }
    resp = session.post(f"{BASE_URL}/2026/search", json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    hits = data.get("hits", [])
    print(f"Found {len(hits)} Music Showcase events")
    return hits


FREE_EVENT_TYPES = ["Activation", "Party", "Exhibition", "Special Event"]


def scrape_free_events(session, csrf_token):
    """Scrape free events: Activations, Parties, Exhibitions, Special Events."""
    headers = {**HEADERS, "X-CSRF-Token": csrf_token}
    all_hits = []
    for etype in FREE_EVENT_TYPES:
        payload = {
            "term": "",
            "filters": [
                {"models": ["event"], "field": "tags", "value": "Free"},
                {"models": ["event"], "field": "event_type", "value": etype},
            ],
            "models": ["event"],
            "page": 1,
        }
        try:
            resp = session.post(f"{BASE_URL}/2026/search", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("hits", [])
            print(f"  Found {len(hits)} Free {etype} events")
            all_hits.extend(hits)
        except Exception as e:
            print(f"  Error scraping Free {etype}: {e}")
    # Deduplicate by _id
    seen = set()
    unique = []
    for h in all_hits:
        hid = h.get("_id", "")
        if hid not in seen:
            seen.add(hid)
            unique.append(h)
    print(f"Found {len(unique)} total Free events (deduplicated)")
    return unique


def scrape_venue(session, venue_id):
    """Scrape venue details including address and coordinates."""
    resp = session.get(
        f"{BASE_URL}/api/web/2026/venues/{venue_id}",
        headers={"User-Agent": HEADERS["User-Agent"], "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


CREDENTIAL_LABELS = {
    "platinum": "Platinum",
    "innovation": "Interactive",
    "interactive": "Interactive",
    "filmtv": "Film & TV",
    "film": "Film & TV",
    "music": "Music",
}


def parse_event(hit, venue_cache):
    """Parse a single event hit into our format."""
    src = hit["_source"]
    venue_info = src.get("venue", {})
    venue_id = venue_info.get("id", "")
    v = venue_cache.get(venue_id, {})
    loc = v.get("location", {})

    # Extract genre from links
    genre = ""
    sponsor = ""
    for link in src.get("links", []):
        if link.get("label") == "Genre" and not genre:
            genre = link.get("value", "")
        if link.get("label") == "Presented By" and not sponsor:
            sponsor = link.get("value", "")

    # Extract tags (e.g., "Free")
    tags = [l["value"] for l in src.get("links", []) if l.get("label") == "Tag"]

    # Parse times from ISO format
    start_iso = src.get("start_time", "")
    end_iso = src.get("end_time", "")
    start_time = ""
    end_time = ""
    if start_iso:
        try:
            dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            # Convert UTC to Central Time (UTC-5 during CDT)
            from datetime import timedelta
            dt_ct = dt - timedelta(hours=5)
            start_time = dt_ct.strftime("%-I:%M%p")
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

    # Map credential types to display labels
    cred_types = src.get("credential_types", [])
    access_levels = list(dict.fromkeys(
        CREDENTIAL_LABELS.get(c, c) for c in cred_types
    ))

    # Add "Free" if in tags
    if "Free" in tags and "Free" not in access_levels:
        access_levels.append("Free")

    address_parts = []
    if loc.get("address"):
        address_parts.append(loc["address"])
    if loc.get("city"):
        address_parts.append(loc["city"])
    if loc.get("state"):
        address_parts[-1] = f"{address_parts[-1]}, {loc['state']}"
    if loc.get("postal_code"):
        address_parts[-1] = f"{address_parts[-1]} {loc['postal_code']}"
    venue_address = ", ".join(address_parts) if address_parts else ""

    lat_lon = loc.get("lat_lon", [None, None])

    # Determine event category
    event_type = src.get("event_type", "")
    event_category = "activation" if event_type in FREE_EVENT_TYPES else "music"

    return {
        "id": hit.get("_id", ""),
        "name": src.get("name", ""),
        "date": src.get("date", ""),
        "start_time": start_time,
        "end_time": end_time,
        "start_iso": start_iso,
        "end_iso": end_iso,
        "venue": venue_info.get("name", ""),
        "venue_id": venue_id,
        "venue_address": venue_address,
        "venue_lat": lat_lon[0] if lat_lon else None,
        "venue_lng": lat_lon[1] if lat_lon else None,
        "access_levels": access_levels,
        "genre": genre,
        "event_type": event_type,
        "event_category": event_category,
        "thumbnail_url": src.get("thumbnail_url", ""),
        "age_policy": v.get("age_policy", ""),
        "sponsor": sponsor,
    }


def scrape_all():
    """Main scraper function."""
    print("Getting session...")
    session, csrf_token = get_session()

    print("Scraping music showcase events...")
    hits = scrape_events(session, csrf_token)

    print("Scraping free events (activations, parties, exhibitions, special events)...")
    free_hits = scrape_free_events(session, csrf_token)

    # Merge and deduplicate
    seen_ids = set(h.get("_id", "") for h in hits)
    for fh in free_hits:
        if fh.get("_id", "") not in seen_ids:
            hits.append(fh)
            seen_ids.add(fh.get("_id", ""))
    print(f"Total unique events: {len(hits)}")

    # Collect unique venue IDs
    venue_ids = set()
    for h in hits:
        vid = h["_source"].get("venue", {}).get("id", "")
        if vid:
            venue_ids.add(vid)

    print(f"Scraping {len(venue_ids)} venues...")
    venue_cache = {}
    for i, vid in enumerate(sorted(venue_ids)):
        try:
            venue_data = scrape_venue(session, vid)
            venue_cache[vid] = venue_data
            loc = venue_data.get("location", {})
            print(f"  [{i+1}/{len(venue_ids)}] {venue_data.get('name', vid)}: {loc.get('address', 'no address')}")
        except Exception as e:
            print(f"  [{i+1}/{len(venue_ids)}] Error scraping {vid}: {e}")
        time.sleep(0.3)  # Be polite

    # Parse all events
    events = [parse_event(h, venue_cache) for h in hits]

    # Collect all genres for the frontend
    genres = sorted(set(e["genre"] for e in events if e["genre"]))

    result = {
        "events": events,
        "genres": genres,
        "venues": {
            vid: {
                "name": v.get("name", ""),
                "address": v.get("location", {}).get("address", ""),
                "city": v.get("location", {}).get("city", ""),
                "state": v.get("location", {}).get("state", ""),
                "lat": v.get("location", {}).get("lat_lon", [None, None])[0],
                "lng": v.get("location", {}).get("lat_lon", [None, None])[1],
                "age_policy": v.get("age_policy", ""),
            }
            for vid, v in venue_cache.items()
        },
        "last_scraped": datetime.utcnow().isoformat(),
    }

    with open("data.json", "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone! Saved {len(events)} events to data.json")
    return result


if __name__ == "__main__":
    scrape_all()
