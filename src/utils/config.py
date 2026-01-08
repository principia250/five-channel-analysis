import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# .envファイルを読み込む
env_path = Path("/opt/airflow/.env")
if not env_path.exists():
    # ホストマシンから実行する場合など、通常の場所を探す
    env_path = Path(".env")
load_dotenv(env_path)


def get_database_url() -> str:
    # DATABASE_URLが設定されている場合はそれを使用
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    
    # ローカル環境（Docker Compose）: デフォルトはpostgres-app:5432（コンテナ内から接続）
    # 本番環境: .envファイルで環境変数を設定して上書き
    user = os.getenv("POSTGRES_APP_USER", "fivech_user")
    password = os.getenv("POSTGRES_APP_PASSWORD", "fivech_password")
    host = os.getenv("POSTGRES_APP_HOST", "postgres-app")  # コンテナ内からはサービス名を使用
    port = os.getenv("POSTGRES_APP_PORT", "5432")  # コンテナ内のポート
    dbname = os.getenv("POSTGRES_APP_DB", "fivech_db")
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


def get_database_url_sync() -> str:
    return get_database_url()


def get_database_config() -> dict:
    # ローカル環境（Docker Compose）: デフォルトはpostgres-app:5432（コンテナ内から接続）
    # 本番環境: .envファイルで環境変数を設定して上書き
    return {
        "user": os.getenv("POSTGRES_APP_USER", "fivech_user"),
        "password": os.getenv("POSTGRES_APP_PASSWORD", "fivech_password"),
        "host": os.getenv("POSTGRES_APP_HOST", "postgres-app"),  # コンテナ内からはサービス名を使用
        "port": int(os.getenv("POSTGRES_APP_PORT", "5432")),  # コンテナ内のポート
        "dbname": os.getenv("POSTGRES_APP_DB", "fivech_db"),
    }

