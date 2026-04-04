import os
from dotenv import load_dotenv

load_dotenv()

API_KEYS = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]


def is_valid_api_key(key: str) -> bool:
    return bool(key) and key in API_KEYS