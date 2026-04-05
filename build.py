#!/usr/bin/env python3
"""
Train Map Builder
Downloads GTFS feeds from GTFS.de and processes them into data.json
"""

import argparse
import csv
import gzip
import io
import json
import re
import shutil
import sys
import zipfile
from math import cos, radians
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import URLError

DATA_DIR = Path("data")
DATA_FILE = Path("data.json")

FEEDS = {
    "fv": {
        "name": "Fernverkehr (Long Distance)",
        "url": "https://download.gtfs.de/germany/fv_free/latest.zip",
        "expected_size": 350_000,
    },
    "rv": {
        "name": "Regionalverkehr",
        "url": "https://download.gtfs.de/germany/rv_free/latest.zip",
        "expected_size": 9_500_000,
    },
}


def download(force: bool = False):
    """Download all feeds. Validates after download."""
    print("\nDownloading GTFS feeds")

    for feed_id in FEEDS:
        feed = FEEDS[feed_id]
        print(f"\n  {feed['name']}:")

        zip_path = DATA_DIR / f"{feed_id}.zip"

        if zip_path.exists() and not force:
            print(f"    Already downloaded ({zip_path.stat().st_size:,} bytes)")
        else:
            print(f"    Downloading from {feed['url']}")

            try:
                temp_path, headers = urlretrieve(feed["url"])
            except URLError as e:
                print(f"    ERROR: Failed to download: {e}")
                sys.exit(1)

            actual_size = Path(temp_path).stat().st_size

            print(f"    Downloaded {actual_size:,} bytes")

            DATA_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(temp_path, zip_path)
            print(f"    Saved to {zip_path}")

        required_files = ["stops.txt", "stop_times.txt", "trips.txt"]
        try:
            with zipfile.ZipFile(zip_path) as zf:
                names = zf.namelist()
                print(f"    Contains {len(names)} files")
                for req in required_files:
                    if req not in names:
                        print(f"    ERROR: Missing required file: {req}")
                        sys.exit(1)
                    print(f"    ✓ {req}")
        except zipfile.BadZipFile as e:
            print(f"    ERROR: Invalid ZIP file: {e}")
            sys.exit(1)


def parse_time(time_str: str) -> int:
    """Parse HH:MM:SS to minutes since midnight."""
    if not time_str or time_str.strip() == "":
        return 0
    parts = time_str.split(":")
    return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) // 60


def should_filter_station(name: str) -> bool:
    # Match narrow gauge platform codes:
    # Benneckenstein Bek_Klb 012 P12
    # Elend Ab_So 001 P1
    if re.search(r" \d{3} P\d+$", name):
        return True

    return False


def lat_lon_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two lat/lon points in km (approximate)."""
    lat_dist = abs(lat1 - lat2) * 111
    lon_dist = abs(lon1 - lon2) * 111 * abs(cos(radians((lat1 + lat2) / 2)))
    return (lat_dist**2 + lon_dist**2) ** 0.5


def is_sub_station(name: str) -> bool:
    """Check if name contains blacklist words indicating it's a sub-station."""
    NON_MAIN = ["gleis", "flixtrain", "tief"]
    return any(x in name.lower() for x in NON_MAIN)


def normalize_station_name(name: str) -> set[str]:
    """Return set of normalized words from station name."""
    import unicodedata

    BH_SYNONYMS = {"bahnhof", "bf", "hbf", "hb"}
    words = re.findall(r"\w+\.?", name.lower())
    normalized = [
        unicodedata.normalize("NFD", w.lower()).encode("ascii", "ignore").decode()
        for w in words
    ]
    filtered = [w for w in normalized if w not in BH_SYNONYMS]
    return set(filtered)


def compare_stations(name1: str, name2: str) -> int:
    """Compare two station names to determine which should be the main station.

    Returns:
        0 if stations do not match
        <0 if they match, prefer name1
        >0 if they match, prefer name2
    """
    has_non_main1 = is_sub_station(name1)
    has_non_main2 = is_sub_station(name2)

    if has_non_main1 != has_non_main2:
        return 1 if has_non_main1 else -1

    remaining1 = normalize_station_name(name1)
    remaining2 = normalize_station_name(name2)

    for w in list(remaining1):
        if w in remaining2:
            remaining1.discard(w)
            remaining2.discard(w)

    for r1, r2 in [(remaining1, remaining2), (remaining2, remaining1)]:
        for w in list(r1):
            if w.endswith("."):
                prefix = w.rstrip(".")
                for w2 in r2:
                    if w2.startswith(prefix):
                        r1.discard(w)
                        r2.discard(w2)
                        break

    if remaining1 or remaining2:
        return 0

    return len(name2) - len(name1)


def get_version_from_feeds() -> str:
    """Get version from zip file modification times."""
    latest_mtime = None

    for feed_id in FEEDS:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        if not zip_path.exists():
            continue

        mtime = zip_path.stat().st_mtime
        if latest_mtime is None or mtime > latest_mtime:
            latest_mtime = mtime

    if latest_mtime:
        return datetime.fromtimestamp(latest_mtime).strftime("%Y%m%d")

    return date.today().strftime("%Y%m%d")


def process(force: bool = False):
    """Process downloaded feeds into data.json."""
    print("\nProcessing connections")

    latest_zip_mtime = 0
    for feed_id in FEEDS:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        if zip_path.exists():
            latest_zip_mtime = max(latest_zip_mtime, zip_path.stat().st_mtime)

    if DATA_FILE.exists() and not force:
        if DATA_FILE.stat().st_mtime >= latest_zip_mtime:
            print("\n  data.json is up to date. Use --force to reprocess.")
            return

    # Build station mappings from both feeds
    print("\n  Building station mappings...")

    stations = {}  # station_id -> {name, lat, lon}
    stop_to_station = {}  # stop_id -> station_id
    filtered_count = 0

    for feed_id in FEEDS:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        print(f"  Processing {feed_id}...")

        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("stops.txt") as f:
                reader = csv.DictReader(f.read().decode("utf-8").splitlines())
                for row in reader:
                    stop_id = row["stop_id"]
                    parent = row.get("parent_station", "")
                    name = row["stop_name"]

                    if should_filter_station(name):
                        filtered_count += 1
                        continue

                    if parent:
                        stop_to_station[stop_id] = parent
                        if parent in stations:
                            stations[parent].setdefault("stops", []).append(stop_id)
                    elif stop_id not in stations:
                        stations[stop_id] = {
                            "sid": stop_id,
                            "name": name,
                            "lat": round(float(row["stop_lat"]), 5),
                            "lon": round(float(row["stop_lon"]), 5),
                            "stops": [stop_id],
                        }
                        stop_to_station[stop_id] = stop_id

    if filtered_count > 0:
        print(f"  Filtered out {filtered_count} stations")

    PROXIMITY_THRESHOLD_KM = 0.15

    station_list = list(stations.values())
    station_list.sort(key=lambda x: x["lat"])

    dedup_count = 0
    for i, info in enumerate(station_list):
        for j in range(i + 1, len(station_list)):
            info2 = station_list[j]
            dist = lat_lon_distance(
                info["lat"], info["lon"], info2["lat"], info2["lon"]
            )
            if dist > PROXIMITY_THRESHOLD_KM:
                break
            sid1, sid2 = info["sid"], info2["sid"]
            if sid1 not in stations or sid2 not in stations:
                continue
            result = compare_stations(info["name"], info2["name"])
            if result == 0:
                print(f"    {info['name']} ≠ {info2['name']}")
            elif result < 0:
                old_stops = stations[sid2]["stops"]
                del stations[sid2]
                stations[sid1]["stops"].extend(old_stops)
                for stop_id in old_stops:
                    stop_to_station[stop_id] = sid1
                dedup_count += 1
                print(f"    {info2['name']} → {info['name']}")
            else:
                old_stops = stations[sid1]["stops"]
                del stations[sid1]
                stations[sid2]["stops"].extend(old_stops)
                for stop_id in old_stops:
                    stop_to_station[stop_id] = sid2
                dedup_count += 1
                print(f"    {info['name']} → {info2['name']}")

    print(
        f"  Total unique stations after filtering & dedup ({dedup_count} merged): {len(stations)}"
    )

    print("\n  Extracting connections...")

    connections = {}  # (station_a, station_b) -> min_time
    for sid in stations:
        connections[sid] = {}

    for feed_id in FEEDS:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        print(f"    Processing {FEEDS[feed_id]['name']}...")

        feed_connections = 0

        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("stop_times.txt") as f:
                reader = csv.DictReader(f.read().decode("utf-8").splitlines())

                current_trip = []
                last_trip_id = None
                last_sequence = -1

                for row in reader:
                    trip_id = row["trip_id"]
                    sequence = int(row["stop_sequence"])
                    if trip_id == last_trip_id:
                        assert sequence > last_sequence, (
                            f"Sequence not incrementing: {last_sequence} -> {sequence}"
                        )
                    last_sequence = sequence

                    if trip_id != last_trip_id and current_trip:
                        for i in range(len(current_trip) - 1):
                            sid_a, mins_a = current_trip[i]
                            destinations = connections[sid_a]
                            for j in range(i + 1, len(current_trip)):
                                sid_b, mins_b = current_trip[j]
                                travel_time = mins_b - mins_a
                                destinations[sid_b] = min(
                                    travel_time, destinations.get(sid_b, 99999)
                                )
                                feed_connections += 1

                        current_trip = []
                        last_sequence = -1

                    stop_id = row["stop_id"]
                    station = stop_to_station.get(stop_id)
                    if station:
                        mins = parse_time(row["arrival_time"])
                        current_trip.append((station, mins))

                    last_trip_id = trip_id

        print(f"      Found {feed_connections:,} connections")

    # Make bidirectional
    for sid_a, destinations in list(connections.items()):
        for sid_b, time in destinations.items():
            if sid_a not in connections[sid_b]:
                connections[sid_b][sid_a] = time
            else:
                connections[sid_b][sid_a] = min(connections[sid_b][sid_a], time)

    # Remove any link to self
    for sid_a, destinations in list(connections.items()):
        destinations.pop(sid_a, None)

    print(
        f"    Total unique connections: {sum(len(d) for d in connections.values()):,}"
    )

    print("\n  Building output...")

    station_list = list(stations.keys())
    station_to_idx = {s: i for i, s in enumerate(station_list)}

    names = []
    coords = []
    edges = []
    edgeTimes = []

    for sid_a in station_list:
        idx_a = station_to_idx[sid_a]
        names.append(stations[sid_a]["name"])
        coords.append([stations[sid_a]["lat"], stations[sid_a]["lon"]])
        edges.append([])
        edgeTimes.append([])
        for sid_b, time in connections[sid_a].items():
            idx_b = station_to_idx[sid_b]
            edges[idx_a].append(idx_b)
            edgeTimes[idx_a].append(time)

    version = get_version_from_feeds()
    print(f"  Feed version: {version}")

    output = {
        "version": version,
        "names": names,
        "coords": coords,
        "edges": edges,
        "edgeTimes": edgeTimes,
    }

    # Write
    with open(DATA_FILE, "w") as f:
        json.dump(output, f)

    # Calculate gzipped size in memory
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(output).encode("utf-8"))
    gzipped_size = buf.tell()

    size = DATA_FILE.stat().st_size
    print(f"\n  Written to {DATA_FILE} ({size:,} bytes, {gzipped_size:,} gzipped)")

    # Find Berlin Hbf index for validation
    berlin_idx = None
    for i, name in enumerate(names):
        if "Berlin" in name and "Hauptbahnhof" in name:
            print(f"\n  Found Berlin station: {i} = '{name}'")
            if berlin_idx is None:
                berlin_idx = i

    return berlin_idx, edges, edgeTimes


def main():
    parser = argparse.ArgumentParser(description="Train Map Builder")
    parser.add_argument(
        "--force", "-f", action="store_true", help="Force re-download or re-processing"
    )
    parser.add_argument(
        "--download-only", action="store_true", help="Only download, skip processing"
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Skip downloading, process existing files only",
    )
    args = parser.parse_args()

    if not args.process_only:
        download(force=args.force)

    if not args.download_only:
        process(force=args.force)

    print("\nDone")


if __name__ == "__main__":
    main()
