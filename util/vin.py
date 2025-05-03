def vins_evaluator(listing):
    """
    Evaluates if a listing matches 'vin's search criteria'.
    """
    # Define vin's search criteria
    allowed_areas = {
        "Tribeca", "Soho", "Gramercy Park", "Chelsea", "Nolita",
        "Greenwich Village", "West Village", "Flatiron", "Financial District"
    }
    max_price = 2700
    max_bedroom_count = 1

    # Check if listing satisfies the criteria
    return (
        listing.get("area_name") in allowed_areas and
        0 <= listing.get("price", float('inf')) <= max_price and
        listing.get("bedroom_count", float('inf')) <= max_bedroom_count
    )

def ellyns_evaluator(listing):
    """
    Evaluates if a listing matches Ellyn's search criteria.
    """
    # Define vin's search criteria
    allowed_areas = {
        "Bedford - Stuyvesant", "Clinton Hill", "Prospect Heights", "Bushwick"
    }

    max_price = 3700
    max_bedroom_count = 2

    # Check if listing satisfies the criteria
    return (
        listing.get("area_name") in allowed_areas and
        0 <= listing.get("price", float('inf')) <= max_price and
        listing.get("bedroom_count", float('inf')) <= max_bedroom_count
    )

def winstons_evaluator(listing):
    """
    Evaluates if a listing matches Winston's search criteria.
    """
    # Define vin's search criteria
    allowed_areas = {
        "Kips Bay", "Astoria", "Hell's Kitchen", "East Village"
    }

    max_price = 3500
    max_bedroom_count = 1
    min_bedroom_count = 0

    # Check if listing satisfies the criteria
    return (
            listing.get("area_name") in allowed_areas and
            0 <= listing.get("price", float('inf')) <= max_price and
            min_bedroom_count <= listing.get("bedroom_count", float('inf')) <= max_bedroom_count
    )

def marnies_evaluator(listing):
    """
    Evaluates if a listing matches Marnie's search criteria.
    """
    # Define vin's search criteria
    allowed_areas = {
        "Williamsburg", "East Williamsburg", "Greenpoint"
    }

    min_price = 2500
    max_price = 3500
    min_bedroom_count = 0

    # Check if listing satisfies the criteria
    return (
            listing.get("area_name") in allowed_areas and
            min_price <= listing.get("price", float('inf')) <= max_price and
            min_bedroom_count < listing.get("bedroom_count", float('inf'))
    )


def evaluate_listing(listing, allowed_areas, min_price=0, max_price=float('inf'), min_bedroom_count=0, max_bedroom_count=float('inf')):
    """
    Evaluates if a listing matches the provided search criteria.
    """
    return (
        listing.get("area_name") in allowed_areas and
        min_price <= listing.get("price", float('inf')) <= max_price and
        min_bedroom_count <= listing.get("bedroom_count", float('inf')) <= max_bedroom_count
    )

if __name__ == "__main__":
    vin_criteria = {
        "allowed_areas": {"Tribeca", "Soho", "Gramercy Park", "Chelsea", "Nolita", "Greenwich Village", "West Village",
                          "Flatiron", "Financial District"},
        "max_price": 2700,
        "max_bedroom_count": 1
    }

    # Check a listing
    listing = {"area_name": "Soho", "price": 2600, "bedroom_count": 1}
    is_match = evaluate_listing(listing, **vin_criteria)
    print(is_match)