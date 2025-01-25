import os
import requests
from fastapi import FastAPI, Depends, HTTPException, Header, Response
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


@app.get("/getBlob")
def get_blob(response: Response):
    # Blob URL
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

        # Return the blob content
        return blob_response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching blob: {str(e)}")

