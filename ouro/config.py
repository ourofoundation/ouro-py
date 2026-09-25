import os

from dotenv import find_dotenv, load_dotenv

# Search from the working directory, not from this file, so an editable install
# doesn't pick up the SDK checkout's own .env.
load_dotenv(find_dotenv(usecwd=True))


class Config:
    DEBUG = os.getenv("DEBUG", "False") == "True"
    OURO_BACKEND_URL = os.getenv(
        "OURO_BACKEND_URL",
        os.getenv("OURO_BASE_URL", "https://api.ouro.foundation"),
    )
