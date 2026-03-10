"""SXSW 2026 Music Showcase Explorer - Flask App"""
import json
import os
import threading
from flask import Flask, render_template, jsonify, request
from geopy.geocoders import Nominatim
from math import radians, cos, sin, asin, sqrt

app = Flask(__name__)
DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")
geocoder = Nominatim(user_agent="sxsw-explorer-2026")


def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return {"events": [], "genres": [], "venues": {}, "last_scraped": None}


def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in miles between two lat/lng points."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * 3956 * asin(sqrt(a))  # 3956 = Earth radius in miles


@app.route("/")
def index():
    data = load_data()
    return render_template(
        "index.html",
        genres=data.get("genres", []),
        last_scraped=data.get("last_scraped", ""),
    )


@app.route("/api/events")
def api_events():
    data = load_data()
    events = data.get("events", [])

    # Optional filters
    day = request.args.get("day")
    genre = request.args.get("genre")
    access = request.args.get("access")  # comma-separated
    search = request.args.get("search", "").lower()

    if day:
        events = [e for e in events if e.get("date") == day]
    if genre:
        genres = genre.split(",")
        events = [e for e in events if e.get("genre") in genres]
    if access:
        access_list = access.split(",")
        # Music Wristband holders can only attend Free events
        if "Music Wristband" in access_list:
            access_list.remove("Music Wristband")
            access_list.append("Free")
        # Deduplicate
        access_list = list(dict.fromkeys(access_list))
        events = [
            e for e in events
            if any(a in e.get("access_levels", []) for a in access_list)
        ]
    if search:
        events = [
            e for e in events
            if search in e.get("name", "").lower()
            or search in e.get("venue", "").lower()
            or search in e.get("genre", "").lower()
        ]

    # Proximity sorting
    user_lat = request.args.get("lat", type=float)
    user_lng = request.args.get("lng", type=float)
    if user_lat is not None and user_lng is not None:
        for e in events:
            if e.get("venue_lat") and e.get("venue_lng"):
                e["distance"] = round(
                    haversine(user_lat, user_lng, e["venue_lat"], e["venue_lng"]), 2
                )
            else:
                e["distance"] = 999
        events.sort(key=lambda e: (e.get("distance", 999), e.get("start_iso") or ""))
    else:
        events.sort(key=lambda e: e.get("start_iso") or "")

    return jsonify({
        "events": events,
        "total": len(events),
        "last_scraped": data.get("last_scraped"),
    })


@app.route("/api/geocode")
def api_geocode():
    address = request.args.get("address", "")
    if not address:
        return jsonify({"error": "No address provided"}), 400
    try:
        location = geocoder.geocode(address + ", Austin, TX")
        if location:
            return jsonify({
                "lat": location.latitude,
                "lng": location.longitude,
                "display": location.address,
            })
        return jsonify({"error": "Address not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/schedule")
def schedule_page():
    data = load_data()
    return render_template(
        "schedule.html",
        last_scraped=data.get("last_scraped", ""),
    )


@app.route("/api/schedule-events", methods=["POST"])
def api_schedule_events():
    """Return full event data for a list of event IDs, sorted chronologically."""
    ids = request.json.get("ids", [])
    if not ids:
        return jsonify({"events": []})
    data = load_data()
    id_set = set(ids)
    events = [e for e in data["events"] if e.get("id") in id_set]
    events.sort(key=lambda e: e.get("start_iso") or "")

    # Calculate walking distances between consecutive events
    for i in range(len(events)):
        events[i]["walk"] = None
        if i > 0:
            prev = events[i - 1]
            curr = events[i]
            if (prev.get("venue_lat") and prev.get("venue_lng")
                    and curr.get("venue_lat") and curr.get("venue_lng")):
                dist = haversine(
                    prev["venue_lat"], prev["venue_lng"],
                    curr["venue_lat"], curr["venue_lng"],
                )
                walk_min = round(dist * 20)  # ~3 mph = 20 min/mile
                events[i]["walk"] = {
                    "miles": round(dist, 2),
                    "minutes": max(walk_min, 1),
                    "from_venue": prev["venue"],
                }

    return jsonify({"events": events})


@app.route("/api/rescrape", methods=["POST"])
def api_rescrape():
    def run_scraper():
        from scraper import scrape_all
        scrape_all()

    thread = threading.Thread(target=run_scraper)
    thread.start()
    return jsonify({"status": "Scraping started"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5555))
    app.run(debug=True, host="0.0.0.0", port=port)
