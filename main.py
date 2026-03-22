from fastapi import FastAPI, Request, Depends, Response
from fastapi.middleware.cors import CORSMiddleware

from util.validate import validate_bearer_token
from util.get_listings import fetch_listings
from util.insert_listings import insert_listings_util
from util.db_queries import get_avg_listings_last_14_days_by_name
from util.check_off_market import check_off_market

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Bloop bloop welcome to the FirstMover API!"}

@app.get("/getListings")
def get_listings(perPage: int = 25, method: str = "v6", _: bool = Depends(validate_bearer_token)):
    return fetch_listings(method=method, per_page=perPage)


@app.post("/insertListings")
def insert_listings(perPage: int = 25, _: bool = Depends(validate_bearer_token)):
    return insert_listings_util(perPage)

@app.post("/getAvgListingsLast14Days")
async def get_avg_listings_last_14_days(request: Request):
    body = await request.json()
    return get_avg_listings_last_14_days_by_name(
        neighborhood_names=body["neighborhood_names"],
        min_price=body["min_price"],
        max_price=body["max_price"],
        bedrooms=body["bedrooms"],
        min_bathroom=body["min_bathroom"]
    )

@app.post("/checkOffMarket")
def check_off_market_endpoint(batchSize: int = 500, _: bool = Depends(validate_bearer_token)):
    return check_off_market(batch_size=batchSize)

@app.options("/getAvgListingsLast14Days")
def options_avg_listings_last_14_days(response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return {}


