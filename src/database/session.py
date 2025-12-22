from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import Base
from src.utils.config import get_database_url

# エンジンの作成
engine = create_engine(
    get_database_url(),
    pool_pre_ping=True,
    echo=False,  # SQLクエリをログ出力する場合はTrueに変更
)

# セッションファクトリの作成
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_db() -> Generator[Session, None, None]:
    yield from get_session()


def init_db() -> None:
    # テスト用途
    Base.metadata.create_all(bind=engine)

