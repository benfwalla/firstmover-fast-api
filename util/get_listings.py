import re
import requests
import logging
import json
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from fastapi import HTTPException
import vercel_blob.blob_store

from util.framer_five import get_framer_five
from util.random_port import get_random_valid_port

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
LISTINGS_BLOB_READ_WRITE_TOKEN = os.getenv("LISTINGS_BLOB_READ_WRITE_TOKEN")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
SCRAPINGFISH_API_KEY = os.getenv("SCRAPINGFISH_API_KEY")

if not all([PROXY_USERNAME, PROXY_PASSWORD]):
    raise ValueError("Missing required proxy credentials in the environment variables.")


def fetch_listings(method="v6", per_page=None):
    """
    Fetch listings from StreetEasy using either API v6 or direct web scraping.
    :param per_page: Number of listings to fetch (only applicable for v6 method)
    :param method: "v6" for API v6 or "web" for web scraping
    """
    if method == "v6":
        response_data = fetch_listings_v6(per_page)
    elif method == "web":
        response_data = fetch_listings_web()
    else:
        raise ValueError("Invalid method. Choose 'v6' or 'web'.")

    framer_five_it(response_data)

    return response_data


def fetch_listings_v6(per_page):
    """ Fetch listings using StreetEasy API v6. """
    url = "https://api-v6.streeteasy.com/"
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
                "perPage": per_page,
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

    # Proxy setup
    random_port = get_random_valid_port()
    proxy_full_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@state.smartproxy.com:{random_port}"
    proxies = {"http": proxy_full_url, "https": proxy_full_url}
    logger.info("Fetching %s listings on Smartproxy port %s", per_page, random_port)

    try:
        response = requests.post(url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
        response_data = response.json()["data"]["searchRentals"]
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch from Streeteasy: %s", e)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

    return response_data


def fetch_listings_web():
    """ Fetch listings directly from StreetEasy website. """
    url = 'https://scraping.narf.ai/api/v1/'
    params = {'api_key': SCRAPINGFISH_API_KEY, 'url': 'https://streeteasy.com/for-rent/nyc?sort_by=listed_desc'}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        html_content = response.text
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch from Streeteasy: %s", e)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

    return parse_web_listings(html_content)


def framer_five_it(response_data):
    """Places the five most recent listings in a blob for marketing site
    Returns the response data as well for future automation."""
    try:
        filtered_data = [
            {
                "id": node.get("id"),
                "areaName": node.get("areaName"),
                "availableAt": node.get("availableAt"),
                "bedroomCount": node.get("bedroomCount"),
                "fullBathroomCount": node.get("fullBathroomCount"),
                "halfBathroomCount": node.get("halfBathroomCount"),
                "noFee": node.get("noFee"),
                "price": node.get("price"),
                "zipCode": node.get("zipCode"),
                "urlPath": node.get("urlPath"),
                "leadMedia": node.get("leadMedia"),
            }
            for edge in response_data.get("edges", []) for node in [edge.get("node", {})]
        ]

        blob_json = get_framer_five(filtered_data)
        vercel_blob.blob_store.put(
            'latest_listings.json',
            json.dumps(blob_json).encode('utf-8'),
            options={"token": LISTINGS_BLOB_READ_WRITE_TOKEN, "addRandomSuffix": False, "cacheControlMaxAge": "0"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BlobError: {e}")

    logger.info("Framer Five blob updated")


def parse_web_listings(html_content):
    """ Parse listings from StreetEasy web page with full response details. """
    soup = BeautifulSoup(html_content, 'html.parser')
    new_listings_url_paths = []
    ul = soup.find('ul', class_='sc-541ed69f-0')
    if not ul:
        return []

    for li in ul.find_all('li', class_='sc-541ed69f-1'):
        if li.find('p', class_='ImageContainerFooter-module__sponsoredTag___pzzz-') or \
                li.find('span', {'data-testid': 'tag-text'}, string='Featured'):
            continue

        address_tag = li.find('a', class_='ListingDescription-module__addressTextAction___xAFZJ')
        if address_tag:
            new_listings_url_paths.append(address_tag['href'].replace("https://streeteasy.com", ""))

    script_tags = soup.find_all('script')
    for script in script_tags:
        if script.string and 'listingData' in script.string:
            try:
                cleaned = re.sub(r'^self\.__next_f\.push\(', '', script.string)
                cleaned = re.sub(r'\)[;\s]*$', '', cleaned)
                cleaned = re.sub(r'\$[A-Za-z0-9_]+', 'null', cleaned)
                cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)

                data = json.loads(cleaned)[1]
                data = data.split(":", 1)[1]
                data = json.loads(data)[3]

                listing_data = data.get("children")[3].get("listingData")
                filtered_edges = []
                for edge in listing_data.get('edges', []):
                    node = edge.get('node', {})
                    url_path = node.get('urlPath')
                    if url_path in new_listings_url_paths:
                        photos = node.get("photos")
                        lead_media = {"photo": {"key": photos[0]["key"]}} if photos else None

                        mapped_node = {
                            "id": node.get("id"),
                            "areaName": node.get("areaName"),
                            "availableAt": node.get("availableAt"),
                            "bedroomCount": node.get("bedroomCount"),
                            "buildingType": node.get("buildingType"),
                            "fullBathroomCount": node.get("fullBathroomCount"),
                            "furnished": node.get("furnished"),
                            "geoPoint": node.get("geoPoint"),
                            "halfBathroomCount": node.get("halfBathroomCount"),
                            "hasTour3d": node.get("hasTour3d"),
                            "hasVideos": node.get("hasVideos"),
                            "isNewDevelopment": node.get("isNewDevelopment"),
                            "leadMedia": lead_media,
                            "leaseTerm": node.get("leaseTerm"),
                            "livingAreaSize": node.get("livingAreaSize"),
                            "mediaAssetCount": len(photos),
                            "monthsFree": node.get("monthsFree"),
                            "noFee": node.get("noFee"),
                            "netEffectivePrice": node.get("netEffectivePrice"),
                            "offMarketAt": node.get("offMarketAt"),
                            "photos": None if not photos else [{"key": p["key"]} for p in photos],
                            "price": node.get("price"),
                            "priceChangedAt": node.get("priceChangedAt"),
                            "priceDelta": node.get("priceDelta"),
                            "sourceGroupLabel": node.get("sourceGroupLabel"),
                            "sourceType": node.get("sourceType"),
                            "state": node.get("state"),
                            "status": node.get("status"),
                            "street": node.get("street"),
                            "upcomingOpenHouse": node.get("upcomingOpenHouse"),
                            "unit": node.get("unit"),
                            "zipCode": node.get("zipCode"),
                            "urlPath": node.get("urlPath")
                        }
                        filtered_edges.append({"node": mapped_node})

                return {
                    'search': listing_data.get('search'),
                    'totalCount': listing_data.get('totalCount'),
                    'edges': filtered_edges,
                    'pageInfo': listing_data.get('pageInfo')
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error: {e}")
    return {}

if __name__ == "__main__":
    print(fetch_listings(method="v6", per_page=10))
    print(fetch_listings(method="web"))
