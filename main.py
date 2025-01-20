import os
from fastapi import FastAPI, HTTPException, Depends, Header
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
PROXY_URL = os.getenv("PROXY_URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

if not all([PROXY_USERNAME, PROXY_PASSWORD, PROXY_URL]):
    raise ValueError("Missing required proxy credentials in the environment variables.")

# Construct the full proxy URL
proxy_full_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_URL}"
proxies = {
    "http": proxy_full_url,
    "https": proxy_full_url,
}

app = FastAPI()


def validate_bearer_token(authorization: str = Header(...)):
    # Check if the header is formatted as "Bearer <token>"
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization header format.")

    token = authorization.split("Bearer ")[1]
    if token != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid Bearer token.")
    return True


@app.get("/getListings")
def get_listings(perPage: int = 10, _: bool = Depends(validate_bearer_token)):

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

    try:
        response = requests.request("POST", url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")



@app.post("/insertListings")
def insert_listings(perPage: int = 10, _: bool = Depends(validate_bearer_token)):
    print(f"Inserting {perPage} listings...")
    return {"message": f"Inserted {perPage} listings successfully!"}
