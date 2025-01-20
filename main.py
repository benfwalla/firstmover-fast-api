import os
from fastapi import FastAPI, Depends, HTTPException, Header
from dotenv import load_dotenv

from util.validate import validate_bearer_token
from util.get_listings import get_listings_util
from util.insert_listings import insert_listings_util

# Load environment variables from .env
load_dotenv()
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
PROXY_URL = os.getenv("PROXY_URL")
CRON_SECRET = os.getenv("CRON_SECRET")

if not all([PROXY_USERNAME, PROXY_PASSWORD, PROXY_URL, CRON_SECRET]):
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
