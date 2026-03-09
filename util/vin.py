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