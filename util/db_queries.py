import os
import logging
from fastapi import HTTPException
from dotenv import load_dotenv
from datetime import datetime, timezone
from supabase import create_client

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_avg_listings_last_14_days_by_name(neighborhood_names, min_price, max_price, bedrooms, min_bathroom):
    """
    Given user inputs of search criteria, return the average number of listings in the last 14 days.
    :return: a float representing the average number of listings (i.e. 22.8667)
    """
    response = supabase.rpc("avg_listings_last_14_days_by_name", {
        "p_neighborhoods": neighborhood_names,
        "p_min_price": min_price,
        "p_max_price": max_price,
        "p_bedrooms": bedrooms,
        "p_min_bathroom": min_bathroom,
        "p_broker_fees": False
    }).execute()

    return response.data

def find_matching_customers(area_name, bedroom_count, bathroom_count, price, broker_fees, zip_code=None):
    """
    Given attributes of a listing, return an array of device tokens of customers that match
    :param area_name:
    :param bedroom_count:
    :param bathroom_count:
    :param price:
    :param broker_fees:
    :param zip_code:
    :return: an array of dictionaries containing 'customer_search_id', 'device_token', and 'user_id'
    """

    if area_name == "Murray Hill":
        if str(zip_code).startswith("11"):
            print("Yoooooo Im in Queens!!")
            area_name = "Murray Hill (Queens)"

    if area_name == "Bay Terrace":
        if str(zip_code).startswith("11"):
            print("Yoooooo Im in Queens!!")
            area_name = "Bay Terrace (Queens)"

    if area_name == "Sunnyside":
        if str(zip_code).startswith("10"):
            print("Enter the Shaolin... Staten Island!")
            area_name = "Sunnyside (Staten Island)"

    if area_name == "Chelsea":
        if str(zip_code).startswith("103"):
            print("Enter the Shaolin... Staten Island!")
            area_name = "Chelsea (Staten Island)"

    response = supabase.rpc("find_matching_customers", {
        "p_area_name": area_name,
        "p_bedroom_count": bedroom_count,
        "p_bathroom_count": bathroom_count,
        "p_price": price,
        "p_no_fee": broker_fees
    }).execute()

    return response.data


def upsert_new_listings(new_listings):
    try:
        response = supabase.table("listings").upsert(new_listings).execute()
        logger.info(f"Supabase upsert successful!")
        return response
    except Exception as e:
        logger.error(f"Error during Supabase upsert: {e}")
        raise HTTPException(status_code=500, detail=f"Supabase Error: {e}")


def upsert_building(building):
    """Upsert a single building into the buildings table."""
    try:
        response = supabase.table("buildings").upsert(building).execute()
        logger.info(f"Building upsert successful: {building['id']}")
        return response
    except Exception as e:
        logger.error(f"Error upserting building {building.get('id')}: {e}")
        return None


def upsert_buildings(buildings):
    """Upsert multiple buildings into the buildings table."""
    if not buildings:
        return None
    try:
        response = supabase.table("buildings").upsert(buildings).execute()
        logger.info(f"Bulk building upsert successful: {len(buildings)} buildings")
        return response
    except Exception as e:
        logger.error(f"Error bulk upserting buildings: {e}")
        return None


def insert_customer_matches(matches_dict: [dict]):
    try:
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            {**match, "created_at": now}
            for match in matches_dict
        ]

        response = supabase.table("customer_matches").insert(payload).execute()
        return response
    except Exception as exception:
        return exception


if __name__ == "__main__":
    print(find_matching_customers("Murray Hill", 1, 1.0, 2362, False, 100016))
    print(find_matching_customers("Murray Hill", 1, 1.0, 2362, False, 11354))
