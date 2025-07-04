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


def match_listings_given_customer_search(customer_search_id, last_x_days=7):
    """
    Given customer_search_id in the database, return an array of listings in the last given amount of days.
    :param customer_search_id:
    :return:
    """
    response = supabase.rpc("get_listings_for_search",{
        "p_customer_search_id": customer_search_id,
        "p_last_x_days": last_x_days
    }).execute()

    return response.data

def get_avg_listings_last_14_days(customer_search_id):
    """
    Given customer_search_id in the database, return the average number of listings in the last 14 days.
    :return: a float representing the average number of listings (i.e. 22.8667)
    """
    response = supabase.rpc("avg_listings_last_14_days",{
        "p_customer_search_id": customer_search_id
    }).execute()

    return response.data


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

def find_matching_customers(area_name, bedroom_count, bathroom_count, price, broker_fees):
    """
    Given user inputs of search criteria, return an array of device tokens of customers that match
    :param area_name:
    :param bedroom_count:
    :param bathroom_count:
    :param price:
    :param broker_fees:
    :return: an array of dictionaries containing 'customer_search_id', 'device_token', and 'user_id'
    """

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
    # print(match_listings_given_customer_search(customer_search_id = 29, last_x_days = 1))
    # print(get_avg_listings_last_14_days(29))
    #
    # avg_listings = get_avg_listings_last_14_days_by_name(
    #     neighborhood_names = ["Kips Bay", "Gramercy Park", "Astoria", "Hell's Kitchen",
    #      "Upper West Side", "Upper East Side", "East Village",
    #      "Prospect Heights", "Clinton Hill"],
    #     min_price = 0,
    #     max_price=3500,
    #     bedrooms=[1, 2],
    #     min_bathroom=1,
    #     broker_fees="fees_ok"
    # )
    #
    # print(avg_listings)
    #
    print(find_matching_customers("West Chelsea", 1, 1.0, 2362, False))

    print(insert_customer_matches([
        {"user_id": "1055a8e4-9ed6-4559-9d71-a2712e25d591", "listing_id": 3923478},
        {"user_id": "1055a8e4-9ed6-4559-9d71-a2712e25d591", "listing_id": 3692174}
    ]))
