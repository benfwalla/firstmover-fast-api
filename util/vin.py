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