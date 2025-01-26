import os
from datetime import datetime
import pytz
import requests
from fastapi import FastAPI, Depends, HTTPException, Response, Query
from dotenv import load_dotenv

from util.validate import validate_bearer_token
from util.get_listings import get_listings_util
from util.insert_listings import insert_listings_util

# Load environment variables from .env
load_dotenv()
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
PROXY_URL = os.getenv("PROXY_URL")

if not all([PROXY_USERNAME, PROXY_PASSWORD, PROXY_URL]):
    raise ValueError("Missing required proxy credentials in the environment variables.")

# Construct the full proxy URL
proxy_full_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}"
proxies = {
    "http": proxy_full_url,
    "https": proxy_full_url,
}

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bloop bloop welcome to the FirstMover API!"}

@app.get("/getListings")
def get_listings(perPage: int = 25, _: bool = Depends(validate_bearer_token)):
    return get_listings_util(perPage, proxies)


@app.post("/insertListings")
def insert_listings(perPage: int = 25, _: bool = Depends(validate_bearer_token)):
    return insert_listings_util(perPage, proxies)


@app.options("/getBlob")
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

        # Parse the JSON and get the first 5 items
        blob_data = blob_response.json()
        limited_data = blob_data[:5] if isinstance(blob_data, list) else blob_data

        # Flatten the listings into a single dictionary at the top level
        flattened_data = {}
        for i, listing in enumerate(limited_data):
            if isinstance(listing, dict):
                for key, value in listing.items():
                    flattened_data[f"{key}{i}"] = value

        # Get the current time in Eastern Time (ET)
        now_et = datetime.now(pytz.timezone("US/Eastern"))
        formatted_time = now_et.strftime("%-m/%-d/%y @ %-I:%M%p").lower() + " ET"

        # Add required CORS headers
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"

        # Return the flattened data directly in the response
        return {
            "message": f"Most recent listings as of {formatted_time}",
            **flattened_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching blob: {str(e)}")



