import requests
from fastapi import HTTPException


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

    try:
        response = requests.request("POST", url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")