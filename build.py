#!/usr/bin/env python3
"""
Train Map Builder
Downloads GTFS feeds from GTFS.de and processes them into data.json
"""

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import zipfile
from collections import defaultdict
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


def download_feed(feed_id: str, force: bool = False) -> Path:
    """Download a GTFS feed if it doesn't exist or force=True."""
    feed = FEEDS[feed_id]
    zip_path = DATA_DIR / f"{feed_id}.zip"
    
    if zip_path.exists() and not force:
        print(f"  {feed['name']}: Already downloaded ({zip_path.stat().st_size:,} bytes)")
        return zip_path
    
    print(f"  {feed['name']}: Downloading...")
    print(f"    from {feed['url']}")
    
    try:
        temp_path, headers = urlretrieve(feed["url"])
    except URLError as e:
        print(f"    ERROR: Failed to download: {e}")
        sys.exit(1)
    
    actual_size = Path(temp_path).stat().st_size
    content_length = headers.get("Content-Length")
    
    print(f"    Downloaded {actual_size:,} bytes")
    
    if content_length:
        expected = int(content_length)
        if abs(actual_size - expected) > 1000:
            print(f"    WARNING: Size mismatch. Expected ~{expected:,}, got {actual_size:,}")
    
    if actual_size < feed["expected_size"] * 0.5:
        print(f"    ERROR: File suspiciously small. Aborting.")
        sys.exit(1)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(temp_path, zip_path)
    print(f"    Saved to {zip_path}")
    
    return zip_path


def validate_zip(zip_path: Path, feed_id: str) -> dict:
    """Validate a downloaded zip and return info about its contents."""
    print(f"\n  Validating {zip_path.name}...")
    
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
            
            return {"valid": True, "files": names}
    except zipfile.BadZipFile as e:
        print(f"    ERROR: Invalid ZIP file: {e}")
        sys.exit(1)


def validate_stops(zip_path: Path, feed_id: str) -> dict:
    """Parse stops.txt and return stats."""
    print(f"\n  Analyzing stops in {feed_id}...")
    
    stations = []
    stops_without_parent = []
    platforms = []
    
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("stops.txt") as f:
            reader = csv.DictReader(f.read().decode("utf-8").splitlines())
            for row in reader:
                location_type = int(row.get("location_type", 0) or 0)
                
                if location_type == 1:
                    stations.append(row)
                elif row.get("parent_station"):
                    platforms.append(row)
                else:
                    stops_without_parent.append(row)
    
    print(f"    Stations (location_type=1): {len(stations)}")
    print(f"    Platforms (with parent_station): {len(platforms)}")
    print(f"    Standalone stops (no parent): {len(stops_without_parent)}")
    
    return {
        "stations": stations,
        "platforms": platforms,
        "standalone": stops_without_parent,
        "total": len(stations) + len(platforms) + len(stops_without_parent)
    }


def download_all(force: bool = False):
    """Download all feeds."""
    print("\nDownloading GTFS feeds")
    
    for feed_id in ["fv", "rv"]:
        feed = FEEDS[feed_id]
        print(f"\n[{feed['name']}]")
        zip_path = download_feed(feed_id, force)
        validate_zip(zip_path, feed_id)
        stops_info = validate_stops(zip_path, feed_id)
        FEEDS[feed_id]["stops_info"] = stops_info


def parse_time(time_str: str) -> int:
    """Parse HH:MM:SS to minutes since midnight."""
    if not time_str or time_str.strip() == '':
        return 0
    parts = time_str.split(':')
    return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) // 60


def build_station_mapping(zip_path: Path) -> tuple:
    """Build mapping from stop_id to station info (stop_id, name, lat, lon)."""
    stop_to_station = {}  # stop_id -> station_stop_id
    stations = {}  # station_stop_id -> {name, lat, lon}
    
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("stops.txt") as f:
            reader = csv.DictReader(f.read().decode("utf-8").splitlines())
            for row in reader:
                stop_id = row["stop_id"]
                location_type = int(row.get("location_type", 0) or 0)
                parent = row.get("parent_station", "")
                name = row["stop_name"]
                
                # Filter out platform-specific stations (narrow gauge codes and P numbers)
                if should_filter_station(name):
                    continue
                
                if location_type == 1:
                    # This is a station
                    stations[stop_id] = {
                        "name": name,
                        "lat": round(float(row["stop_lat"]), 5),
                        "lon": round(float(row["stop_lon"]), 5),
                    }
                    stop_to_station[stop_id] = stop_id
                elif parent:
                    # Platform with parent - map to parent (includes "Gleis" platforms)
                    stop_to_station[stop_id] = parent
                else:
                    # Standalone stop - use as-is
                    stations[stop_id] = {
                        "name": name,
                        "lat": round(float(row["stop_lat"]), 5),
                        "lon": round(float(row["stop_lon"]), 5),
                    }
                    stop_to_station[stop_id] = stop_id
    
    return stop_to_station, stations


def should_filter_station(name: str) -> bool:
    """Filter out stations with platform-specific metadata patterns.
    
    Filters out:
    - Narrow gauge platform codes (e.g., Bek_Klb, Wdk_Wdh, Eld_Ab)
    - Platform numbers (e.g., " P1", " P7")
    
    Note: "Gleis" filtering is handled separately with location_type awareness
    to avoid filtering legitimate heritage/narrow-gauge station names.
    """
    # Match narrow gauge platform codes: uppercase_uppercase (e.g., Bek_Klb, Eld_Ab)
    if re.search(r'[A-Z]{2,}_[A-Z]', name):
        return True
    
    # Match platform numbers: space + P + digit (e.g., " P1", " P7")
    if re.search(r' P\d', name):
        return True
    
    return False


def deduplicate_stations(stations: dict) -> tuple[dict, dict, list]:
    """Deduplicate stations based on proximity (same coordinates within 0.001 degrees ~100m).
    
    Returns: (deduped_stations, old_to_new_mapping, deduplication_log)
    """
    # Filter out stations with platform metadata patterns
    filtered = {sid: info for sid, info in stations.items() if not should_filter_station(info['name'])}
    
    groups = defaultdict(list)
    for sid, info in filtered.items():
        # Group by rounded coordinates (0.001 degree ~ 100m at mid-latitudes)
        key = (round(info['lat'], 3), round(info['lon'], 3))
        groups[key].append(sid)
    
    deduped = {}
    old_to_new = {}
    dedups = []  # List of (merged_stations, chosen_station)
    
    for group in groups.values():
        if len(group) == 1:
            sid = group[0]
            deduped[sid] = stations[sid]
            old_to_new[sid] = sid
        else:
            # Choose station with "Hauptbahnhof" in name if available, else first
            chosen = group[0]
            for sid in group:
                if "Hauptbahnhof" in stations[sid]['name']:
                    chosen = sid
                    break
            deduped[chosen] = stations[chosen]
            for sid in group:
                old_to_new[sid] = chosen
            
            # Log this deduplication
            merged_names = [stations[sid]['name'] for sid in group if sid != chosen]
            dedups.append((merged_names, stations[chosen]['name']))
    
    return deduped, old_to_new, dedups


def extract_connections(zip_path: Path, stop_to_station: dict) -> dict:
    """Extract connections from stop_times.txt.
    
    A 'direct' connection means reachable without transfers.
    So all stations on the same route/trip are directly connected to each other.
    """
    from itertools import combinations
    
    connections = {}  # (station_a, station_b) -> min_time_minutes
    
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("stop_times.txt") as f:
            reader = csv.DictReader(f.read().decode("utf-8").splitlines())
            
            trips = {}
            for row in reader:
                trip_id = row["trip_id"]
                if trip_id not in trips:
                    trips[trip_id] = []
                trips[trip_id].append({
                    "stop_id": row["stop_id"],
                    "sequence": int(row["stop_sequence"]),
                    "arrival": row["arrival_time"],
                    "departure": row["departure_time"],
                })
    
    print(f"    Processing {len(trips):,} trips...")
    
    for trip_id, stops in trips.items():
        stops.sort(key=lambda x: x["sequence"])
        
        trip_stations = []
        for stop in stops:
            station = stop_to_station.get(stop["stop_id"])
            if station and (not trip_stations or trip_stations[-1] != station):
                trip_stations.append(station)
        
        if len(trip_stations) < 2:
            continue
        
        for a, b in combinations(trip_stations, 2):
            a_idx = next((i for i, s in enumerate(stops) if stop_to_station.get(s["stop_id"]) == a), None)
            b_idx = next((i for i, s in enumerate(stops) if stop_to_station.get(s["stop_id"]) == b), None)
            
            if a_idx is None or b_idx is None:
                continue
            
            earlier_idx, later_idx = min(a_idx, b_idx), max(a_idx, b_idx)
            earlier_dep = parse_time(stops[earlier_idx]["departure"])
            later_arr = parse_time(stops[later_idx]["arrival"])
            travel_time = later_arr - earlier_dep
            
            if 1 <= travel_time <= 480:
                key = (a, b)
                if key not in connections or travel_time < connections[key]:
                    connections[key] = travel_time
    
    return connections


def get_version_from_feeds() -> str:
    """Get version from zip file modification times."""
    latest_mtime = None
    
    for feed_id in ["fv", "rv"]:
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
    for feed_id in ["fv", "rv"]:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        if zip_path.exists():
            latest_zip_mtime = max(latest_zip_mtime, zip_path.stat().st_mtime)
    
    if DATA_FILE.exists() and not force:
        if DATA_FILE.stat().st_mtime >= latest_zip_mtime:
            print(f"\n  data.json is up to date. Use --force to reprocess.")
            return
    
    # Build station mappings from both feeds
    print("\n  Building station mappings...")
    
    all_stations = {}  # station_id -> {name, lat, lon}
    all_stop_to_station = {}  # stop_id -> station_id
    filtered_count = 0  # Stations filtered by pattern
    
    for feed_id in ["fv", "rv"]:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        print(f"    Processing {feed_id}...")
        
        stop_to_station, stations = build_station_mapping(zip_path)
        
        # Filter out platform-specific stations
        for sid in list(stations.keys()):
            if should_filter_station(stations[sid]['name']):
                del stations[sid]
                filtered_count += 1
        
        # Merge
        for stop_id, station_id in stop_to_station.items():
            all_stop_to_station[stop_id] = station_id
        
        for station_id, info in stations.items():
            if station_id not in all_stations:
                all_stations[station_id] = info
            # Keep first occurrence
    
    if filtered_count > 0:
        print(f"    Filtered out {filtered_count} platform-specific stations")
    
    # Deduplicate stations based on proximity
    all_stations, old_to_new, dedups = deduplicate_stations(all_stations)
    
    # Log deduplications
    if dedups:
        print(f"\n  Deduplication results ({len(dedups)} groups merged):")
        for merged_names, chosen_name in sorted(dedups):
            for merged in merged_names:
                print(f"    '{merged}' → '{chosen_name}'")
    
    # Update stop_to_station mappings
    for stop_id, station_id in all_stop_to_station.items():
        mapped = old_to_new.get(station_id, station_id)
        if mapped in all_stations:  # Only include if station wasn't filtered
            all_stop_to_station[stop_id] = mapped
    
    print(f"    Total unique stations after filtering & dedup: {len(all_stations)}")
    
    # Extract connections from both feeds
    print("\n  Extracting connections...")
    
    all_connections = {}  # (station_a, station_b) -> min_time
    
    for feed_id in ["fv", "rv"]:
        zip_path = DATA_DIR / f"{feed_id}.zip"
        print(f"    Processing {feed_id}...")
        
        connections = extract_connections(zip_path, all_stop_to_station)
        
        for key, time in connections.items():
            if key not in all_connections or time < all_connections[key]:
                all_connections[key] = time
        
        print(f"      Found {len(connections):,} connections")
    
    print(f"    Total unique connections: {len(all_connections):,}")
    
    # Make bidirectional and convert to output format
    print("\n  Building output...")
    
    station_list = list(all_stations.keys())
    station_to_idx = {s: i for i, s in enumerate(station_list)}
    num_stations = len(station_list)
    
    connection_times = [{} for _ in range(num_stations)]  # For deduplication
    
    for (a_id, b_id), time in all_connections.items():
        a_idx = station_to_idx.get(a_id)
        b_idx = station_to_idx.get(b_id)
        
        if a_idx is None or b_idx is None:
            continue
        
        # A -> B (deduplicate)
        if b_idx not in connection_times[a_idx] or time < connection_times[a_idx][b_idx]:
            connection_times[a_idx][b_idx] = time
        
        # B -> A (bidirectional, deduplicate)
        if a_idx not in connection_times[b_idx] or time < connection_times[b_idx][a_idx]:
            connection_times[b_idx][a_idx] = time
    
    # Convert to sorted lists
    print("  Converting to output format...")
    
    # Flatten edges: edges[i] = [dest1, dest2, ...], edgeTimes[i] = [time1, time2, ...]
    edges = []
    edgeTimes = []
    
    for idx in range(num_stations):
        conn_list = [[dest, t] for dest, t in connection_times[idx].items()]
        conn_list.sort(key=lambda x: x[1])
        edges.append([dest for dest, _ in conn_list])
        edgeTimes.append([t for _, t in conn_list])
    
    # Build output
    names = [all_stations[sid]["name"] for sid in station_list]
    coords = [[all_stations[sid]["lat"], all_stations[sid]["lon"]] for sid in station_list]
    
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
    
    size = DATA_FILE.stat().st_size
    print(f"\n  Written to {DATA_FILE} ({size:,} bytes)")
    
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
    parser.add_argument("--force", "-f", action="store_true", 
                        help="Force re-download of existing files")
    parser.add_argument("--download-only", action="store_true",
                        help="Only download, skip processing")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate existing downloads")
    parser.add_argument("--process-only", action="store_true",
                        help="Skip downloading, process existing files only")
    args = parser.parse_args()
    
    if args.validate_only:
        print("\nValidating downloads")
        for feed_id in ["fv", "rv"]:
            zip_path = DATA_DIR / f"{feed_id}.zip"
            if not zip_path.exists():
                print(f"\n  {feed_id}: No file found at {zip_path}")
                continue
            validate_zip(zip_path, feed_id)
            validate_stops(zip_path, feed_id)
        print("\n  Validation complete.")
        return
    
    if not args.process_only:
        download_all(force=args.force)
    
    if not args.download_only:
        process(force=args.force)
    
    print("\nDone")


if __name__ == "__main__":
    main()
