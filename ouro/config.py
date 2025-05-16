import os

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


class Config:
    DEBUG = os.getenv("DEBUG", "False") == "True"
    SUPABASE_URL = "https://database.ouro.foundation"
    SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlzcyI6InN1cGFiYXNlIiwiaWF0IjoxNzE0OTc1MjAwLCJleHAiOjE4NzI3NDE2MDB9.qFUUaJ8m-hPNAM0VlU1QiqLN80c6twhP7Ok9EJkNMNw"
    OURO_BACKEND_URL = "https://api.ouro.foundation"
