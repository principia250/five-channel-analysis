import os
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy import JSON, Integer

from src.database.models import Base


def _replace_postgresql_types_for_sqlite():
    """SQLite用にPostgreSQL固有の型（JSONB, ARRAY, BigInteger）を適切な型に置き換える"""
    from sqlalchemy import BigInteger
    # すべてのテーブルのカラムを走査してPostgreSQL固有の型を置き換え
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, (JSONB, ARRAY)):
                column.type = JSON()
            # SQLite用にBigIntegerをIntegerに置き換え（autoincrement対応）
            elif isinstance(column.type, BigInteger) and column.primary_key:
                column.type = Integer()


@pytest.fixture(scope="session")
def test_engine():
    """テスト用のデータベースエンジン"""
    # 環境変数からテスト用DB接続文字列を取得（デフォルトはSQLite）
    test_db_url = os.getenv(
        "TEST_DATABASE_URL",
        "sqlite:///:memory:",
    )
    
    use_sqlite = test_db_url.startswith("sqlite")
    
    if use_sqlite:
        # SQLiteの場合（軽量だが制限あり）
        # PostgreSQL固有の型（JSONB, ARRAY）をJSONに置き換える
        _replace_postgresql_types_for_sqlite()
        
        engine = create_engine(
            test_db_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        
        # SQLiteで外部キー制約を有効化
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    else:
        # PostgreSQLの場合（docker-composeのpostgres-appを使用）
        engine = create_engine(
            test_db_url,
            pool_pre_ping=True,
        )
    
    # テーブルを作成
    Base.metadata.create_all(engine)
    yield engine
    
    # テーブルを削除
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def test_session(test_engine):
    """各テスト用の独立したセッション（トランザクションでロールバック）"""
    connection = test_engine.connect()
    
    # SQLiteの場合、外部キー制約を有効化（各接続ごとに必要）
    if test_engine.dialect.name == "sqlite":
        # PRAGMAステートメントを実行（autobeginを開始する）
        connection.execute(text("PRAGMA foreign_keys=ON"))
        # PRAGMAのために開始されたトランザクションをコミット
        connection.commit()
    
    transaction = connection.begin()
    
    SessionLocal = sessionmaker(bind=connection)
    session = SessionLocal()
    
    yield session
    
    session.close()
    # トランザクションがまだ有効な場合のみロールバック
    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.fixture
def sample_html_board_page():
    """サンプル板ページHTML"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "fixtures",
        "sample_html",
        "board_page.html",
    )
    with open(fixture_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def sample_html_thread_page():
    """サンプルスレッドページHTML"""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "fixtures",
        "sample_html",
        "thread_page.html",
    )
    with open(fixture_path, "r", encoding="utf-8") as f:
        return f.read()

