import os
import json
import logging
import sys
import time
import uuid

from dotenv import load_dotenv
from fastapi import HTTPException
from upstash_redis import Redis
from supabase import create_client, Client

from util.get_listings import fetch_listings
from util.push_notification import send_push_notification
from util.db_queries import (
    find_matching_customers,
    insert_customer_matches,
    normalize_streeteasy_area_name,
    upsert_new_listings,
)
from util.check_off_market import fetch_listing_statuses, fetch_and_upsert_buildings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Redis configuration
KV_REST_API_URL = os.getenv("KV_REST_API_URL")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN")
if not KV_REST_API_URL or not KV_REST_API_TOKEN:
    logger.error("Missing Redis configuration")
    raise ValueError("Missing Redis configuration")

redis = Redis(url=KV_REST_API_URL, token=KV_REST_API_TOKEN)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing Supabase configuration")
    raise ValueError("Missing Supabase configuration")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _log_event(level, event, run_id, **fields):
    """Emit one compact, searchable event to Vercel Runtime Logs."""
    payload = {"event": event, "run_id": run_id, **fields}
    getattr(logger, level)(json.dumps(payload, default=str, separators=(",", ":")))


def insert_listings_util(per_page):
    run_id = uuid.uuid4().hex[:8]
    started_at = time.monotonic()
    stats = {
        "stage": "starting",
        "source": None,
        "fetched": 0,
        "new": 0,
        "saved": 0,
        "linked_to_existing_building": 0,
        "linked_to_new_building": 0,
        "not_linked_to_building": 0,
    }
    _log_event("info", "listing_scrape_started", run_id, per_page=per_page)

    try:
        result = _insert_listings_util(per_page, run_id, stats)
    except Exception as error:
        logger.exception(json.dumps({
            "event": "listing_scrape_failed",
            "run_id": run_id,
            "duration_ms": round((time.monotonic() - started_at) * 1000),
            "error_type": type(error).__name__,
            **stats,
        }, default=str, separators=(",", ":")))
        raise

    _log_event(
        "info",
        "listing_scrape_complete",
        run_id,
        duration_ms=round((time.monotonic() - started_at) * 1000),
        **stats,
    )
    return result


def _insert_listings_util(per_page, run_id, stats):
    # Try to fetch listings using v6 API first
    stats["stage"] = "fetch_listings"
    methods = [
        {"name": "v6", "params": {"method": "v6", "per_page": per_page}},
        {"name": "web", "params": {"method": "web"}}
    ]
    for method in methods:
        try:
            fetched_data = fetch_listings(**method["params"])
            edges = fetched_data.get("edges", [])
            stats["source"] = method["name"]
            stats["fetched"] = len(edges)
            break
        except Exception as e:
            logger.warning(f"{method['name']} method failed: {e}")
            if method["name"] == "web":  # If this was the last method to try
                logger.error("All fetch methods failed")
                raise HTTPException(status_code=500, detail="Error fetching listings from all methods")

    latest_ids = [edge["node"]["id"] for edge in edges]

    stats["stage"] = "compare_redis_checkpoint"
    try:
        # Get last IDs from Redis
        last_ids_raw = redis.get("last_ids")
        last_ids = last_ids_raw.split(",") if last_ids_raw else []
        # Find new IDs (not present in last_ids from Redis)
        new_ids = [id for id in latest_ids if id not in last_ids]
        stats["new"] = len(new_ids)
        _log_event(
            "info",
            "new_listings_discovered",
            run_id,
            count=len(new_ids),
            listing_ids=new_ids,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail="Error doing Redis comparison")

    # Prepare new listings for upsert
    new_listings = []
    new_matches = []
    customer_match_counts = {}
    stats["stage"] = "prepare_listings"

    for edge in edges:
        node = edge["node"]
        if node.get("id") in new_ids:
            area_name = normalize_streeteasy_area_name(
                node.get("areaName"),
                node.get("zipCode"),
            )
            listing = {
                "id": node.get("id"),
                "area_name": area_name,
                "available_at": node.get("availableAt"),
                "bedroom_count": node.get("bedroomCount"),
                "building_type": node.get("buildingType"),
                "full_bathroom_count": node.get("fullBathroomCount"),
                "furnished": node.get("furnished"),
                "latitude": node.get("geoPoint", {}).get("latitude"),
                "longitude": node.get("geoPoint", {}).get("longitude"),
                "half_bathroom_count": node.get("halfBathroomCount"),
                "has_tour_3d": node.get("hasTour3d"),
                "has_videos": node.get("hasVideos"),
                "is_new_development": node.get("isNewDevelopment"),
                "lease_term": node.get("leaseTerm"),
                "living_area_size": node.get("livingAreaSize"),
                "media_asset_count": node.get("mediaAssetCount"),
                "months_free": node.get("monthsFree"),
                "no_fee": node.get("noFee"),
                "net_effective_price": node.get("netEffectivePrice"),
                "off_market_at": node.get("offMarketAt"),
                "price": node.get("price"),
                "price_changed_at": node.get("priceChangedAt"),
                "price_delta": node.get("priceDelta"),
                "source_group_label": node.get("sourceGroupLabel"),
                "source_type": node.get("sourceType"),
                "state": node.get("state"),
                "status": node.get("status"),
                "street": node.get("street"),
                "unit": node.get("unit"),
                "zip_code": node.get("zipCode"),
                "url_path": node.get("urlPath"),
                "lead_media_photo": (node.get("leadMedia") or {}).get("photo", {}).get("key"),
                "photos": ",".join(photo.get("key", "") for photo in (node.get("photos") or [])),
                "upcoming_open_house_start": (node.get("upcomingOpenHouse") or {}).get("startTime"),
                "upcoming_open_house_end": (node.get("upcomingOpenHouse") or {}).get("endTime"),
                "upcoming_open_house_appointment_only": (node.get("upcomingOpenHouse") or {}).get("appointmentOnly"),
            }

            new_listings.append(listing)

            total_bathrooms = listing.get("full_bathroom_count", 0) + (listing.get("half_bathroom_count", 0)*0.5)
            total_bathrooms = int(total_bathrooms) if total_bathrooms.is_integer() else total_bathrooms

            bedroom_display = "Studio" if listing.get("bedroom_count", 0) == 0 else f"{listing['bedroom_count']} Bed"

            matched_customers = find_matching_customers(
                listing["area_name"],
                listing["bedroom_count"],
                total_bathrooms,
                listing["price"],
                not listing.get("no_fee", False),
                listing.get("zip_code"))
            customer_match_counts[listing["id"]] = len(matched_customers)

            if matched_customers:

                # Send push notifications
                matched_customers_device_tokens = [customer["device_token"] for customer in matched_customers]
                send_push_notification(
                    to=matched_customers_device_tokens,
                    title=f"New Listing in {listing['area_name']}",
                    body=f"${listing['price']:,} | {bedroom_display} | {total_bathrooms} Bath",
                    data_url=f"https://streeteasy.com{listing['url_path']}",
                    listing_id=listing['id']
                )

                new_matches.extend(
                    {"user_id": customer["user_id"], "listing_id": listing["id"]}
                    for customer in matched_customers
                )

    # Bulk fetch building IDs for all new listings (1 API call instead of N)
    building_outcomes = {listing["id"]: "lookup_failed" for listing in new_listings}
    if new_listings:
        stats["stage"] = "match_buildings"
        try:
            new_listing_ids = [l["id"] for l in new_listings]
            se_data = fetch_listing_statuses(new_listing_ids)
            if se_data is not None:
                # Collect building IDs we need to fetch
                building_ids_needed = set()
                listing_to_building = {}
                for listing in new_listings:
                    listing_id = listing["id"]
                    info = se_data.get(listing_id)
                    if not info:
                        building_outcomes[listing_id] = "listing_not_returned"
                    elif not info.get("building_id"):
                        building_outcomes[listing_id] = "no_building_id"
                    else:
                        building_id = info["building_id"]
                        building_ids_needed.add(building_id)
                        listing_to_building[listing_id] = building_id

                # Bulk fetch and upsert buildings (1 API call)
                if building_ids_needed:
                    _, known_building_ids, new_building_ids = fetch_and_upsert_buildings(building_ids_needed)

                    # Set building_id on listings (only if building exists in DB)
                    for listing in new_listings:
                        bid = listing_to_building.get(listing["id"])
                        if bid and bid in known_building_ids:
                            listing["building_id"] = bid
                            building_outcomes[listing["id"]] = (
                                "created" if bid in new_building_ids else "existing"
                            )
                        elif bid:
                            building_outcomes[listing["id"]] = "building_fetch_failed"
        except Exception as e:
            logger.warning(f"Bulk building fetch failed, continuing without building data: {e}")

        stats["stage"] = "save_listings"
        upsert_new_listings(new_listings)
        stats["saved"] = len(new_listings)

        for listing in new_listings:
            outcome = building_outcomes[listing["id"]]
            if outcome == "existing":
                stats["linked_to_existing_building"] += 1
            elif outcome == "created":
                stats["linked_to_new_building"] += 1
            else:
                stats["not_linked_to_building"] += 1

            _log_event(
                "info" if outcome in {"existing", "created"} else "warning",
                "listing_processed",
                run_id,
                listing_id=listing["id"],
                area_name=listing["area_name"],
                zip_code=listing["zip_code"],
                saved=True,
                building_id=listing.get("building_id"),
                building_match=outcome,
                customers_matched=customer_match_counts.get(listing["id"], 0),
            )

        stats["stage"] = "save_redis_checkpoint"
        redis.set("last_ids", ",".join(latest_ids))

    if new_matches:
        stats["stage"] = "save_customer_matches"
        insert_customer_matches(new_matches)

    stats["stage"] = "complete"
    logger.debug("New listings: %s", new_listings)
    return {"newListings": new_listings}


if __name__ == "__main__":
    print(insert_listings_util(10))
