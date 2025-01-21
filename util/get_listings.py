import requests
from fastapi import HTTPException
import vercel_blob.blob_store
import json
import os
from dotenv import load_dotenv

load_dotenv()
BLOB_READ_WRITE_TOKEN = os.getenv("BLOB_READ_WRITE_TOKEN")


def get_listings_util(perPage, proxies):
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

    # Make Request to Streeteasy
    try:
        response = requests.request("POST", url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

    # Blob Storage
    try:
        filtered_data = [
            {
                "id": node["node"]["id"],
                "areaName": node["node"]["areaName"],
                "availableAt": node["node"]["availableAt"],
                "buildingType": node["node"]["buildingType"],
                "price": node["node"]["price"],
                "leadMedia": node["node"]["leadMedia"],
                "unit": node["node"]["unit"],
                "urlPath": node["node"]["urlPath"],
                "noFee": node["node"]["noFee"]
            }
            for node in response.json()["data"]["searchRentals"]["edges"]
        ]

        resp = vercel_blob.blob_store.put('latest_listings.json',
                                          json.dumps(filtered_data).encode('utf-8'),
                                          options={"token": BLOB_READ_WRITE_TOKEN,
                                                   "addRandomSuffix": False,
                                                   "cacheControlMaxAge": "0"}
                                          )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BlobError: {e}")


    return response.json()


if __name__ == "__main__":
    print(get_listings_util(100, None))