import os
from dotenv import load_dotenv
from fastapi import HTTPException
from upstash_redis import Redis
from supabase import create_client, Client
from util.get_listings import get_listings_util

# Load environment variables
load_dotenv()

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL")
REDIS_TOKEN = os.getenv("REDIS_TOKEN")
if not REDIS_URL or not REDIS_TOKEN:
    raise ValueError("Missing Redis configuration")

redis = Redis(url=REDIS_URL, token=REDIS_TOKEN)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase configuration")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def insert_listings_util(perPage, proxies):
    fetched_data = get_listings_util(perPage, proxies)

    latest_ids = [
        edge["node"]["id"]
        for edge in fetched_data["data"]["searchRentals"].get("edges", [])
    ]

    # Get last 25 IDs from Redis
    last_ids_raw = redis.get("last_25_ids")
    last_ids = last_ids_raw.split(",") if last_ids_raw else []

    print("Last IDs:", last_ids)
    # Find new IDs (not present in last 25 IDs)
    new_ids = [id for id in latest_ids if id not in last_ids]

    print("New IDs:", new_ids)

    # Prepare new listings for upsert
    new_listings = [
        {
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
            "lead_media_photo": node.get("leadMedia", {}).get("photo", {}).get("key"),
            "photos": ",".join(photo.get("key", "") for photo in node.get("photos", [])),
            "upcoming_open_house_start": (node.get("upcomingOpenHouse") or {}).get("startTime"),
            "upcoming_open_house_end": (node.get("upcomingOpenHouse") or {}).get("endTime"),
            "upcoming_open_house_appointment_only": (node.get("upcomingOpenHouse") or {}).get("appointmentOnly"),
        }
        for edge in fetched_data["data"]["searchRentals"].get("edges", [])
        for node in [edge["node"]]
        if node.get("id") in new_ids
    ]

    if new_listings:

        try:
            response = supabase.table("listings").upsert(new_listings).execute()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Supabase Error: {e}")

        redis.set("last_25_ids", ",".join(latest_ids))

    return { "newListings": new_listings }