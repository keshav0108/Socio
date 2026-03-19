import os
from dotenv import load_dotenv

load_dotenv()

API_KEYS = os.getenv("API_KEYS", "").split(",")

def is_valid_api_key(key: str):
    return key in API_KEYS