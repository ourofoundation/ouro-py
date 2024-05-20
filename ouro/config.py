import os

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


class Config:
    DEBUG = os.getenv("DEBUG", "False") == "True"
    SUPABASE_URL = os.getenv("SUPABASE_URL", "https://database.ouro.foundation")
    SUPABASE_ANON_KEY = os.getenv(
        "SUPABASE_ANON_KEY",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0",
    )
    OURO_BACKEND_URL = os.getenv("OURO_BACKEND_URL", "https://api.ouro.foundation")
