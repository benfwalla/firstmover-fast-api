"""
Backfill buildings for existing listings that don't have a building_id.

Strategy:
1. Get distinct (street, zip_code) combos from listings without building_id
2. Pick one listing_id per unique address
3. Call buildingByRentalListingId concurrently (NO proxy — direct API, proven safe)
4. Deduplicate buildings by ID before bulk upserting
5. Bulk update listings with building_id

Usage (from project root):
    python -m scripts.backfill_buildings --batch-size 1000 --workers 1 --delay 0.5
"""

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from supabase import create_client

from util.get_building import fetch_building_by_listing_id
from util.db_queries import upsert_buildings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_addresses_without_buildings(limit=1000):
    """Get unique (street, zip_code) pairs with a sample listing_id, where building_id is null."""
    try:
        response = supabase.rpc("get_distinct_addresses_without_buildings", {"p_limit": limit}).execute()
        if response.data:
            return [
                {"listing_id": row["listing_id"], "street": row["street"], "zip_code": row["zip_code"]}
                for row in response.data
            ]
    except Exception:
        pass

    # Fallback: paginated fetch + deduplicate in Python
    all_rows = []
    page_size = 1000
    offset = 0
    target_rows = limit * 5

    while len(all_rows) < target_rows:
        response = (
            supabase.table("listings")
            .select("id, street, zip_code")
            .is_("building_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not response.data:
            break
        all_rows.extend(response.data)
        offset += page_size
        if len(response.data) < page_size:
            break

    seen = {}
    for row in all_rows:
        key = (row["street"], row["zip_code"])
        if key not in seen:
            seen[key] = row["id"]

    addresses = [
        {"listing_id": lid, "street": s, "zip_code": z}
        for (s, z), lid in seen.items()
    ]
    return addresses[:limit]


def bulk_update_listings_building_id(updates):
    """Batch update listings with building_id."""
    total = 0
    for u in updates:
        try:
            response = (
                supabase.table("listings")
                .update({"building_id": u["building_id"]})
                .eq("street", u["street"])
                .eq("zip_code", u["zip_code"])
                .is_("building_id", "null")
                .execute()
            )
            total += len(response.data) if response.data else 0
        except Exception as e:
            logger.error(f"Failed to update listings at {u['street']}, {u['zip_code']}: {e}")
    return total


def fetch_one(addr, use_proxy=True):
    """Fetch building for a single address."""
    try:
        building = fetch_building_by_listing_id(addr["listing_id"], use_proxy=use_proxy)
        return (addr, building)
    except Exception as e:
        logger.warning(f"Error fetching building for listing {addr['listing_id']}: {e}")
        return (addr, None)


def deduplicate_buildings(buildings):
    """Remove duplicate buildings by ID, keeping the first occurrence."""
    seen = {}
    for b in buildings:
        if b["id"] not in seen:
            seen[b["id"]] = b
    return list(seen.values())


def backfill(batch_size=500, workers=1, delay=0.3):
    """Run the backfill process with concurrent API calls."""
    logger.info(f"Starting backfill: batch_size={batch_size}, workers={workers}, delay={delay}s")

    addresses = get_addresses_without_buildings(limit=batch_size)
    if not addresses:
        logger.info("No listings without buildings found. Done!")
        return

    logger.info(f"Found {len(addresses)} unique addresses to process")

    buildings_to_upsert = []
    listings_to_update = []
    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for i, addr in enumerate(addresses):
            future = executor.submit(fetch_one, addr, use_proxy=True)
            futures[future] = addr

            if delay > 0 and i < len(addresses) - 1:
                time.sleep(delay)

        for future in as_completed(futures):
            addr, building = future.result()
            if building:
                buildings_to_upsert.append(building)
                listings_to_update.append({
                    "street": addr["street"],
                    "zip_code": addr["zip_code"],
                    "building_id": building["id"],
                })
                successes += 1
            else:
                failures += 1

            done = successes + failures
            if done % 100 == 0:
                logger.info(f"  Progress: {done}/{len(addresses)} ({successes} ok, {failures} failed)")

    # Deduplicate buildings by ID before upserting
    unique_buildings = deduplicate_buildings(buildings_to_upsert)
    logger.info(f"Upserting {len(unique_buildings)} unique buildings (from {len(buildings_to_upsert)} total)...")

    for i in range(0, len(unique_buildings), 100):
        chunk = unique_buildings[i:i+100]
        upsert_buildings(chunk)

    # Only update listings whose building was successfully upserted
    upserted_ids = {b["id"] for b in unique_buildings}
    valid_updates = [u for u in listings_to_update if u["building_id"] in upserted_ids]

    logger.info(f"Updating listings with building_id ({len(valid_updates)} addresses)...")
    total_updated = bulk_update_listings_building_id(valid_updates)

    logger.info(
        f"Backfill complete: {len(unique_buildings)} buildings upserted, "
        f"{total_updated} listings updated, {failures} API failures"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill building data for listings")
    parser.add_argument("--batch-size", type=int, default=1000, help="Unique addresses to process per run")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent API requests")
    parser.add_argument("--delay", type=float, default=0.3, help="Seconds between submitting requests")
    args = parser.parse_args()

    backfill(batch_size=args.batch_size, workers=args.workers, delay=args.delay)
