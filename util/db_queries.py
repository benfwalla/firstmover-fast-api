import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

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


def get_avg_listings_last_14_days_by_name(neighborhood_names, min_price, max_price, bedrooms, min_bathroom,
                                          broker_fees):
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
        "p_broker_fees": broker_fees
    }).execute()

    return response.data

# TODO: Add a function where given a new listing, we query all of the customer searches and the ids and FCM tokens of the users


if __name__ == "__main__":
    print(match_listings_given_customer_search(customer_search_id = 29, last_x_days = 1))
    print(get_avg_listings_last_14_days(29))

    avg_listings = get_avg_listings_last_14_days_by_name(
        neighborhood_names = ["Kips Bay", "Gramercy Park", "Astoria", "Hell's Kitchen",
         "Upper West Side", "Upper East Side", "East Village",
         "Prospect Heights", "Clinton Hill"],
        min_price = 0,
        max_price=3500,
        bedrooms=[1, 2],
        min_bathroom=1,
        broker_fees="fees_ok"
    )

    print(avg_listings)
