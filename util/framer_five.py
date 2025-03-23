from datetime import datetime
import pytz

def get_framer_five(listings_dict):
    framer_five = listings_dict
    flattened_data = {}
    for i, listing in enumerate(framer_five):
        if isinstance(listing, dict):
            for key, value in listing.items():
                flattened_data[f"{key}{i}"] = value

    now_et = datetime.now(pytz.timezone("US/Eastern"))
    formatted_time = now_et.strftime("%-m/%-d/%y @ %-I:%M%p").lower() + " ET"

    return {
        "message": f"Most recent listings as of {formatted_time}",
        **flattened_data
    }
