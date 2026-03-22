import logging
import os
import sys

import requests
from dotenv import load_dotenv
from util.random_port import get_random_valid_port

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

SE_API_URL = "https://api-v6.streeteasy.com/"

SE_HEADERS = {
    "sec-ch-ua-platform": '"macOS"',
    "x-forwarded-proto": "https",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "app-version": "1.0.0",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "accept": "application/json",
    "apollographql-client-version": "version  83ee85d5ed42e55b57081e8a6505003a62af073b",
    "content-type": "application/json",
    "apollographql-client-name": "srp-frontend-service",
    "os": "web",
    "dnt": "1",
    "origin": "https://streeteasy.com",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "referer": "https://streeteasy.com/",
    "accept-language": "en-US,en;q=0.9",
}

BUILDING_FIELDS = """
    id
    slug
    name
    description
    address { street city state zipCode }
    geoCenter { latitude longitude }
    yearBuilt
    floorCount
    totalUnitCount
    residentialUnitCount
    type
    status
    amenities {
        list
        doormanTypes
        parkingTypes
        sharedOutdoorSpaceTypes
        storageSpaceTypes
    }
    policies {
        list
        petPolicy {
            catsAllowed
            dogsAllowed
            maxDogWeight
            restrictedDogBreeds
        }
    }
    rentalInventorySummary {
        featureSummary {
            list
        }
    }
    nyc {
        bin
        bbl
        buildingClass
        buildingClassDescription
        hasAbatements
        schoolDistrict
    }
"""


def _get_proxy():
    random_port = get_random_valid_port()
    proxy_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@state.smartproxy.com:{random_port}"
    return {"http": proxy_url, "https": proxy_url}


def _parse_building(raw):
    """Convert a raw GraphQL building response into a flat dict for Supabase."""
    if not raw:
        return None

    address = raw.get("address") or {}
    geo = raw.get("geoCenter") or {}
    amenities = raw.get("amenities") or {}
    policies = raw.get("policies") or {}
    pet_policy = policies.get("petPolicy") or {}
    feature_summary = ((raw.get("rentalInventorySummary") or {}).get("featureSummary") or {}).get("list") or []
    nyc = raw.get("nyc") or {}

    return {
        "id": raw.get("id"),
        "slug": raw.get("slug"),
        "name": raw.get("name") or None,
        "description": raw.get("description") or None,
        "street": address.get("street"),
        "city": address.get("city"),
        "state": address.get("state"),
        "zip_code": address.get("zipCode"),
        "latitude": geo.get("latitude"),
        "longitude": geo.get("longitude"),
        "year_built": raw.get("yearBuilt"),
        "floor_count": raw.get("floorCount"),
        "total_unit_count": raw.get("totalUnitCount"),
        "residential_unit_count": raw.get("residentialUnitCount"),
        "building_type": raw.get("type"),
        "building_status": raw.get("status"),
        "amenities": amenities.get("list") or [],
        "doorman_types": amenities.get("doormanTypes") or [],
        "parking_types": amenities.get("parkingTypes") or [],
        "shared_outdoor_spaces": amenities.get("sharedOutdoorSpaceTypes") or [],
        "storage_types": amenities.get("storageSpaceTypes") or [],
        "policies": policies.get("list") or [],
        "pets_cats_allowed": pet_policy.get("catsAllowed"),
        "pets_dogs_allowed": pet_policy.get("dogsAllowed"),
        "pets_max_dog_weight": pet_policy.get("maxDogWeight"),
        "common_unit_features": feature_summary,
        "bin": nyc.get("bin"),
        "bbl": nyc.get("bbl"),
        "building_class": nyc.get("buildingClass"),
        "building_class_description": nyc.get("buildingClassDescription"),
        "has_abatements": nyc.get("hasAbatements") or False,
        "school_district": nyc.get("schoolDistrict"),
    }


def fetch_building_by_listing_id(listing_id, use_proxy=True):
    """Fetch a single building by rental listing ID. Returns parsed dict or None."""
    payload = {
        "query": f"""
            query GetBuildingByListingId($id: ID!) {{
                buildingByRentalListingId(id: $id) {{
                    {BUILDING_FIELDS}
                }}
            }}
        """,
        "variables": {"id": str(listing_id)}
    }

    try:
        proxies = _get_proxy() if use_proxy else None
        response = requests.post(SE_API_URL, headers=SE_HEADERS, json=payload, proxies=proxies, timeout=10)
        response.raise_for_status()
        data = response.json()
        raw = data.get("data", {}).get("buildingByRentalListingId")
        return _parse_building(raw)
    except Exception as e:
        logger.warning(f"Failed to fetch building for listing {listing_id}: {e}")
        return None


def fetch_buildings_by_ids(building_ids):
    """Fetch multiple buildings by their building IDs. Returns list of parsed dicts."""
    if not building_ids:
        return []

    payload = {
        "query": f"""
            query GetBuildingsByIds($ids: [ID!]!) {{
                buildingsByIds(ids: $ids) {{
                    {BUILDING_FIELDS}
                }}
            }}
        """,
        "variables": {"ids": [str(bid) for bid in building_ids]}
    }

    try:
        response = requests.post(SE_API_URL, headers=SE_HEADERS, json=payload, proxies=_get_proxy(), timeout=30)
        response.raise_for_status()
        data = response.json()
        raw_buildings = data.get("data", {}).get("buildingsByIds") or []
        return [_parse_building(b) for b in raw_buildings if b]
    except Exception as e:
        logger.warning(f"Failed to bulk fetch buildings: {e}")
        return []
