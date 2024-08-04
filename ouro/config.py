import os

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


class Config:
    DEBUG = os.getenv("DEBUG", "False") == "True"
    SUPABASE_URL = os.getenv("SUPABASE_URL", "https://database.ouro.foundation")
    SUPABASE_ANON_KEY = os.getenv(
        "SUPABASE_ANON_KEY",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzIyNjY4NDAwLAogICJleHAiOiAxODgwNDM0ODAwCn0.48r75lJf_ByLzHb8GccNkW9qAhfrY0OIDUf40Jc1OsA",
    )
    OURO_BACKEND_URL = os.getenv("OURO_BACKEND_URL", "https://api.ouro.foundation")
