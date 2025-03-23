import requests
from fastapi import FastAPI, Request, Depends, HTTPException, Response

from util.validate import validate_bearer_token
from util.get_listings import fetch_listings
from util.insert_listings import insert_listings_util
from util.db_queries import get_avg_listings_last_14_days_by_name

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bloop bloop welcome to the FirstMover API!"}

@app.get("/getListings")
def get_listings(perPage: int = 25, _: bool = Depends(validate_bearer_token)):
    return fetch_listings(method="v6", per_page=perPage)


@app.post("/insertListings")
def insert_listings(perPage: int = 25, _: bool = Depends(validate_bearer_token)):
    return insert_listings_util(perPage)

@app.post("/getAvgListingsLast14Days")
async def get_avg_listings_last_14_days(
    request: Request,
    _: bool = Depends(validate_bearer_token)
):
    body = await request.json()
    return get_avg_listings_last_14_days_by_name(
        neighborhood_names=body["neighborhood_names"],
        min_price=body["min_price"],
        max_price=body["max_price"],
        bedrooms=body["bedrooms"],
        min_bathroom=body["min_bathroom"],
        broker_fees=body["broker_fees"]
    )

@app.options("/getFramerBlob")
def options_blob(response: Response):
    # Handle preflight `OPTIONS` requests for /getBlob
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return {}


@app.get("/getFramerBlob")
def get_blob(response: Response):

    blob_url = "https://591qwi72as9qsy9j.public.blob.vercel-storage.com/latest_listings.json"

    try:
        # Fetch the blob content using requests
        blob_response = requests.get(blob_url)

        if blob_response.status_code != 200:
            raise HTTPException(
                status_code=blob_response.status_code,
                detail=f"Failed to fetch blob: {blob_response.reason}",
            )

        # Add required CORS headers
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"

        # Return the flattened data directly in the response
        return blob_response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching blob: {str(e)}")

