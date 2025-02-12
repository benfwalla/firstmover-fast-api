from datetime import datetime

import pytz
import requests
import logging
from fastapi import HTTPException
import vercel_blob.blob_store
import json
import os
from dotenv import load_dotenv

from util.framer_five import get_framer_five
from util.random_port import get_random_valid_port

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()
LISTINGS_BLOB_READ_WRITE_TOKEN = os.getenv("LISTINGS_BLOB_READ_WRITE_TOKEN")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

if not all([PROXY_USERNAME, PROXY_PASSWORD]):
    raise ValueError("Missing required proxy credentials in the environment variables.")


def get_listings_util(perPage):
    payload = {
        "query": """
            query GetAllRentalListingDetails($input: SearchRentalsInput!) {
                searchRentals(input: $input) {
                    search {
                        criteria
                    }
                    totalCount
                    edges {
                        ... on OrganicRentalEdge {
                            node {
                                id
                                areaName
                                availableAt
                                bedroomCount
                                buildingType
                                fullBathroomCount
                                furnished
                                geoPoint {
                                    latitude
                                    longitude
                                }
                                halfBathroomCount
                                hasTour3d
                                hasVideos
                                isNewDevelopment
                                leadMedia {
                                    photo {
                                        key
                                    }
                                    floorPlan {
                                        key
                                    }
                                    video {
                                        imageUrl
                                        id
                                        provider
                                    }
                                    tour3dUrl
                                }
                                leaseTerm
                                livingAreaSize
                                mediaAssetCount
                                monthsFree
                                noFee
                                netEffectivePrice
                                offMarketAt
                                photos {
                                    key
                                }
                                price
                                priceChangedAt
                                priceDelta
                                sourceGroupLabel
                                sourceType
                                state
                                status
                                street
                                upcomingOpenHouse {
                                    startTime
                                    endTime
                                    appointmentOnly
                                }
                                unit
                                zipCode
                                urlPath
                            }
                        }
                    }
                }
            }
        """,
        "variables": {
            "input": {
                "filters": {
                    "rentalStatus": "ACTIVE",
                    "areas": [1]
                },
                "page": 1,
                "perPage": perPage,
                "sorting": {
                    "attribute": "LISTED_AT",
                    "direction": "DESCENDING"
                },
                "userSearchToken": "3142397c-a6e6-4bb8-a6d3-330e1db1bf85",
                "adStrategy": "NONE"
            }
        }
    }

    headers = {
        "sec-ch-ua-platform": '"macOS"',
        "x-forwarded-proto": "https",
        "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "app-version": "1.0.0",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "accept": "application/json",
        "apollographql-client-version": "version  83ee85d5ed42e55b57081e8a6505003a62af073b",
        "content-type": "application/json",
        "apollographql-client-name": "srp-frontend-service",
        "os": "web",
        "dnt": "1",
        "origin": "https://streeteasy.com",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://streeteasy.com/",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
    }

    url = "https://api-v6.streeteasy.com/"

    # Construct the full proxy URL
    random_port = get_random_valid_port()
    proxy_full_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@state.smartproxy.com:{random_port}"
    proxies = {
        "http": proxy_full_url,
        "https": proxy_full_url,
    }
    logger.info("Fetching %s listings on Smartproxy port %s", perPage, random_port)

    # Make Request to Streeteasy
    try:
        response = requests.request("POST", url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch from Streeteasy: %s", e)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

    # Blob Storage
    try:
        filtered_data = []
        for edge in response.json()["data"]["searchRentals"]["edges"]:
            node = edge["node"]
            filtered_data.append({
                "photo": f"https://photos.zillowstatic.com/fp/{node['leadMedia']['photo']['key']}-se_large_800_400.webp" if node.get(
                    "leadMedia") and node["leadMedia"].get("photo") else None,
                "url": f"https://streeteasy.com{node.get('urlPath')}" if node.get("urlPath") else None,
                "topLine": (
                    f"{'${:,.0f}'.format(node.get('price')) if node.get('price') else 'Price not available'} | "
                    f"{'No Fee' if node.get('noFee') else 'Fee Likely'} | "
                    f"{node.get('areaName')}" if node.get("areaName") else None
                ),

                "bedBathDisplay": (
                    f"{'Studio' if node.get('bedroomCount', 0) == 0 else f'{node.get('bedroomCount', 0)} Bed'} | "
                    f"{f'{int(node.get('fullBathroomCount', 0))} Bath' if node.get('halfBathroomCount', 0) == 0 else f'{node.get('fullBathroomCount', 0) + node.get('halfBathroomCount', 0) * 0.5:.1f} Bath'}"
                )
            })

        blob_json = get_framer_five(filtered_data)

        resp = vercel_blob.blob_store.put('latest_listings.json',
                                          json.dumps(blob_json).encode('utf-8'),
                                          options={"token": LISTINGS_BLOB_READ_WRITE_TOKEN,
                                                   "addRandomSuffix": False,
                                                   "cacheControlMaxAge": "0"}
                                          )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BlobError: {e}")


    return response.json()


if __name__ == "__main__":
    print(get_listings_util(10))