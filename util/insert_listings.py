import os
import logging
from dotenv import load_dotenv
from fastapi import HTTPException
from upstash_redis import Redis
from supabase import create_client, Client
from util.get_listings import get_listings_util
from util.vin import vins_evaluator
from util.telegram import send_to_telegram

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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

# Telegram configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


def insert_listings_util(perPage, proxies):
    logger.info("Fetching listings with perPage=%s", perPage)

    try:
        fetched_data = get_listings_util(perPage, proxies)
        edges = fetched_data["data"]["searchRentals"].get("edges", [])
        logger.info(f"Fetched {len(edges)} listings")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error fetching listings")

    latest_ids = [edge["node"]["id"] for edge in edges]

    # Get last IDs from Redis
    last_ids_raw = redis.get("last_ids")
    last_ids = last_ids_raw.split(",") if last_ids_raw else []
    logger.debug("Last IDs: %s", last_ids)

    # Find new IDs (not present in last IDs)
    new_ids = [id for id in latest_ids if id not in last_ids]

    # Prepare new listings for upsert
    new_listings = []
    for edge in edges:
        node = edge["node"]
        if node.get("id") in new_ids:
            listing = {
                "id": node.get("id"),
                "area_name": node.get("areaName"),
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

            telegram_message = (
                f"${listing['price']:,} | {'Fee Likely' if not listing.get('no_fee', False) else 'No Fee'} | {listing['area_name']}\n"
                f"{listing['bedroom_count']} Bed | {listing.get('full_bathroom_count', 0)} Bath\n"
                f"<a href='https://streeteasy.com{listing['url_path']}'>View Listing</a>"
            )

            send_to_telegram(-4731252559, telegram_message, TELEGRAM_BOT_TOKEN)

            if vins_evaluator(listing):
                send_to_telegram(1138345693, telegram_message, TELEGRAM_BOT_TOKEN)


    logger.info(f"Prepared {len(new_listings)} new listings for upsert")

    if new_listings:
        try:
            response = supabase.table("listings").upsert(new_listings).execute()
            logger.info(f"Supabase upsert successful!")
        except Exception as e:
            logger.error(f"Error during Supabase upsert: {e}")
            raise HTTPException(status_code=500, detail=f"Supabase Error: {e}")

        redis.set("last_ids", ",".join(latest_ids))

    logger.debug("New listings: %s", new_listings)
    return {"newListings": new_listings}
