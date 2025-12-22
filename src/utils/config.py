import os
from typing import Optional
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()


def get_database_url() -> str:
    user = os.getenv("POSTGRES_APP_USER", "fivech_user")
    password = os.getenv("POSTGRES_APP_PASSWORD", "fivech_password")
    host = os.getenv("POSTGRES_APP_HOST", "localhost")
    port = os.getenv("POSTGRES_APP_PORT", "5433")
    dbname = os.getenv("POSTGRES_APP_DB", "fivech_db")
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def get_database_url_sync() -> str:
    return get_database_url()


def get_database_config() -> dict:
    return {
        "user": os.getenv("POSTGRES_APP_USER", "fivech_user"),
        "password": os.getenv("POSTGRES_APP_PASSWORD", "fivech_password"),
        "host": os.getenv("POSTGRES_APP_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_APP_PORT", "5433")),
        "dbname": os.getenv("POSTGRES_APP_DB", "fivech_db"),
    }

