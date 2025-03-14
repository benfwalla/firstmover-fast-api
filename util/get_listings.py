import requests
import logging
from fastapi import HTTPException
import vercel_blob.blob_store
import json
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup

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
SCRAPINGFISH_API_KEY = os.getenv("SCRAPINGFISH_API_KEY")

if not all([PROXY_USERNAME, PROXY_PASSWORD]):
    raise ValueError("Missing required proxy credentials in the environment variables.")


def get_listings_api_v6(perPage):
    """Gets the most recent {perPage} listings from StreetEasy using api-v6.streeteasy.com"""
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


def get_listings_web():
    """Gets the most recent listings from StreetEasy directly from the website"""
    url = 'https://scraping.narf.ai/api/v1/'
    params = {
        'api_key': SCRAPINGFISH_API_KEY,
        'url': 'https://streeteasy.com/for-rent/nyc?sort_by=listed_desc'
    }
    response = requests.get(url, params=params)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    listings = []

    ul = soup.find('ul', class_='sc-541ed69f-0')
    if not ul:
        return []

    for li in ul.find_all('li', class_='sc-541ed69f-1'):
        # Skip Sponsored and Featured listings
        if li.find('p', class_='ImageContainerFooter-module__sponsoredTag___pzzz-') or \
           li.find('span', {'data-testid': 'tag-text'}, string='Featured'):
            continue

        listing = {}

        # Extract URL and Address
        address_tag = li.find('a', class_='ListingDescription-module__addressTextAction___xAFZJ')
        if address_tag:
            listing['street'] = address_tag.text.strip()
            listing['url_path'] = address_tag['href'].replace("https://streeteasy.com", "")

        # Extract Area Name
        area_tag = li.find('p', class_='Caps_base_LkkqI')
        if area_tag:
            listing['area_name'] = area_tag.text.strip().split(' in ')[-1]

        # Extract Price
        price_tag = li.find('span', class_='PriceInfo-module__price___gf-El')
        if price_tag:
            listing['price'] = int(price_tag.text.replace('$', '').replace(',', '').strip())

        # Extract No Fee status
        no_fee_tag = li.find('span', {'data-testid': 'tag-text'}, string='NO FEE')
        listing['no_fee'] = bool(no_fee_tag)

        # Extract Bedroom and Bathroom Counts
        bed_bath_list = li.find_all('span', class_='BedsBathsSqft-module__text___lnveO')
        listing['bedroom_count'] = None
        listing['full_bathroom_count'] = None
        listing['half_bathroom_count'] = 0

        if bed_bath_list:
            for item in bed_bath_list:
                text = item.text.strip()
                if 'Studio' in text:
                    listing['bedroom_count'] = 0
                elif 'bed' in text:
                    listing['bedroom_count'] = int(text.split(' ')[0])
                elif 'bath' in text:
                    try:
                        bath_count = float(text.split(' ')[0])
                        listing['full_bathroom_count'] = int(bath_count)
                        listing['half_bathroom_count'] = int((bath_count - int(bath_count)) * 2)
                    except ValueError:
                        listing['full_bathroom_count'] = None
                        listing['half_bathroom_count'] = 0

        # Extract Lead Media Photo
        image_tag = li.find('img', class_='CardImage-module__cardImage___cirIN')
        if image_tag:
            listing['lead_media_photo'] = image_tag['src']

        listings.append(listing)

    return listings




if __name__ == "__main__":
    #print(get_listings_api_v6(10))
    print(get_listings_web())