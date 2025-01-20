from fastapi import HTTPException, Header
import os
from dotenv import load_dotenv

load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")


def validate_bearer_token(authorization: str = Header(...)):
    # Check if the header is formatted as "Bearer <token>"
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization header format.")

    token = authorization.split("Bearer ")[1]
    if token != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid Bearer token.")
    return True
