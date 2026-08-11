import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def get_access_token() -> str:
  
    # 1) Env var override (useful for prod / secrets managers)
    env_token = os.getenv("UPSTOX_ACCESS_TOKEN")
    if env_token:
        logger.info("Using Upstox access token from environment variable.")
        return env_token

    # 2) JSON file next to this script: utils/upstox_token.json
    token_path = Path(__file__).with_name("upstox_token.json")

    if not token_path.exists():
        raise Exception(f"Access token missing or token file not found at: {token_path}")

    try:
        with token_path.open("r") as f:
            data = json.load(f)
    except Exception as e:
        raise Exception(f"Failed to read token file {token_path}: {e}")

    access_token = data.get("access_token")

    if not access_token:
        raise Exception(f"'access_token' key missing or empty in {token_path}")

    logger.info("Successfully loaded Upstox access token from JSON file.")
    return access_token