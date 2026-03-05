import os

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


class Config:
    DEBUG = os.getenv("DEBUG", "False") == "True"
    OURO_BACKEND_URL = "https://api.ouro.foundation"
