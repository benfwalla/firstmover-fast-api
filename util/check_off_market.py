import logging
import os
import sys

import requests
from dotenv import load_dotenv
from supabase import create_client

from util.get_building import SE_HEADERS, BUILDING_FIELDS, _parse_building, _get_proxy

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SE_API_URL = "https://api-v6.streeteasy.com/"


def fetch_listing_statuses(listing_ids):
    """Bulk fetch listing statuses + building IDs from StreetEasy.
    Returns dict of {listing_id: {status, offMarketAt, buildingId}} for listings SE still knows about.
    """
    if not listing_ids:
        return {}

    payload = {
        "query": """query CheckListingStatus($ids: [ID!]!) {
            rentalsByListingIds(ids: $ids) {
                id buildingId status offMarketAt
            }
        }""",
        "variables": {"ids": [str(id) for id in listing_ids]}
    }

    # Retry once with a different proxy port on failure
    for attempt in range(2):
        try:
            response = requests.post(SE_API_URL, headers=SE_HEADERS, json=payload, proxies=_get_proxy(), timeout=30)
            response.raise_for_status()
            data = response.json()

            if "data" not in data:
                logger.warning(f"Unexpected response (attempt {attempt + 1}): {str(data)[:200]}")
                continue

            listings = data["data"]["rentalsByListingIds"]
            return {
                l["id"]: {
                    "status": l["status"],
                    "off_market_at": l.get("offMarketAt"),
                    "building_id": l.get("buildingId"),
                }
                for l in listings
            }
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")

    logger.error("Failed to fetch listing statuses after 2 attempts")
    return None  # None = API failed, {} = API succeeded but returned no matches


def fetch_and_upsert_buildings(building_ids):
    """Fetch full building data for IDs we don't have yet, upsert them.
    Returns (count_added, set_of_all_known_ids) so callers know which building_ids are safe to link.
    """
    if not building_ids:
        return 0, set()

    # Check which buildings we already have
    existing = supabase.table("buildings").select("id").in_("id", list(building_ids)).execute()
    existing_ids = {r["id"] for r in existing.data}
    new_ids = [bid for bid in building_ids if bid not in existing_ids]

    if not new_ids:
        return 0, existing_ids

    logger.info(f"Fetching {len(new_ids)} new buildings via buildingsByIds")

    payload = {
        "query": f"""query GetBuildingsByIds($ids: [ID!]!) {{
            buildingsByIds(ids: $ids) {{
                {BUILDING_FIELDS}
            }}
        }}""",
        "variables": {"ids": new_ids}
    }

    try:
        response = requests.post(SE_API_URL, headers=SE_HEADERS, json=payload, proxies=_get_proxy(), timeout=30)
        response.raise_for_status()
        data = response.json()
        raw_buildings = data.get("data", {}).get("buildingsByIds") or []

        buildings = [_parse_building(b) for b in raw_buildings if b]
        if buildings:
            seen = {}
            for b in buildings:
                if b["id"] not in seen:
                    seen[b["id"]] = b
            unique = list(seen.values())

            for i in range(0, len(unique), 100):
                chunk = unique[i:i+100]
                supabase.table("buildings").upsert(chunk).execute()

            upserted_ids = {b["id"] for b in unique}
            logger.info(f"Upserted {len(unique)} new buildings")
            return len(unique), existing_ids | upserted_ids
    except Exception as e:
        logger.error(f"Failed to fetch/upsert buildings: {e}")

    return 0, existing_ids


def check_off_market(batch_size=500):
    """
    Check ACTIVE listings for off-market status and backfill building IDs.

    1. Get batch of oldest ACTIVE listings from DB
    2. Bulk check their status via rentalsByListingIds (one API call)
    3. Update status + off_market_at for any that changed
    4. Fetch and upsert any new buildings discovered
    5. Backfill building_id on listings
    """
    # Get oldest ACTIVE listings
    response = (
        supabase.table("listings")
        .select("id, building_id, created_at")
        .eq("status", "ACTIVE")
        .order("created_at")
        .limit(batch_size)
        .execute()
    )
    listing_rows = response.data
    if not listing_rows:
        logger.info("No ACTIVE listings to check")
        return {"checked": 0, "off_market": 0, "expired": 0, "buildings_added": 0, "buildings_linked": 0}

    listing_ids = [r["id"] for r in listing_rows]
    listings_missing_building = {r["id"] for r in listing_rows if not r.get("building_id")}
    listing_dates = {r["id"]: r.get("created_at") for r in listing_rows}

    logger.info(f"Checking {len(listing_ids)} listings ({len(listings_missing_building)} missing building_id)")

    # Bulk fetch statuses from StreetEasy
    se_data = fetch_listing_statuses(listing_ids)
    if se_data is None:
        logger.error("SE API call failed — skipping this batch entirely to avoid false expires")
        return {"checked": len(listing_ids), "se_returned": 0, "expired": 0, "off_market": 0, "buildings_added": 0, "buildings_linked": 0, "skipped": True}
    logger.info(f"StreetEasy returned data for {len(se_data)} out of {len(listing_ids)} listings")

    # Separate: status updates + building ID collection
    status_updates = []
    building_ids_to_fetch = set()
    building_id_updates = []

    for listing_id, info in se_data.items():
        # Status changed?
        if info["status"] != "ACTIVE":
            status_updates.append({
                "id": listing_id,
                "status": info["status"],
                "off_market_at": info["off_market_at"],
            })

        # Building ID available and listing needs it?
        if info.get("building_id") and listing_id in listings_missing_building:
            building_ids_to_fetch.add(info["building_id"])
            building_id_updates.append({
                "id": listing_id,
                "building_id": info["building_id"],
            })

    # Mark listings not returned by SE as EXPIRED (they've been purged)
    returned_ids = set(se_data.keys())
    not_returned = list(set(listing_ids) - returned_ids)
    expired_count = 0
    if not_returned:
        logger.info(f"{len(not_returned)} listings not returned by StreetEasy — marking as EXPIRED")
        # Batch update in chunks of 100
        for i in range(0, len(not_returned), 100):
            chunk = not_returned[i:i+100]
            try:
                supabase.table("listings").update({
                    "status": "EXPIRED",
                }).in_("id", chunk).execute()
                expired_count += len(chunk)
            except Exception as e:
                logger.error(f"Failed to mark expired listings: {e}")

    # 1. Update off-market statuses
    off_market_count = 0
    for update in status_updates:
        try:
            supabase.table("listings").update({
                "status": update["status"],
                "off_market_at": update["off_market_at"],
            }).eq("id", update["id"]).execute()
            off_market_count += 1
        except Exception as e:
            logger.error(f"Failed to update listing {update['id']}: {e}")

    logger.info(f"Updated {off_market_count} listings to off-market")

    # 2. Fetch and upsert new buildings
    buildings_added, known_building_ids = fetch_and_upsert_buildings(building_ids_to_fetch)

    # 3. Backfill building_id on listings (only for buildings we know exist)
    buildings_linked = 0
    for update in building_id_updates:
        if update["building_id"] not in known_building_ids:
            continue
        try:
            supabase.table("listings").update({
                "building_id": update["building_id"],
            }).eq("id", update["id"]).execute()
            buildings_linked += 1
        except Exception as e:
            logger.error(f"Failed to link building on listing {update['id']}: {e}")

    logger.info(f"Linked {buildings_linked} listings to buildings")

    result = {
        "checked": len(listing_ids),
        "se_returned": len(se_data),
        "expired": expired_count,
        "off_market": off_market_count,
        "buildings_added": buildings_added,
        "buildings_linked": buildings_linked,
    }
    logger.info(f"Check complete: {result}")
    return result
