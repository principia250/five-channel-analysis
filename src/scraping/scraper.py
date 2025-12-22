import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.scraping import utils


class Scraper:
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        request_delay: float = 2.0,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.request_delay = request_delay

        # セッションの設定
        self.session = requests.Session()
        self.session.headers.update(utils.get_default_headers())

        # リトライ戦略の設定
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def fetch(self, url: str, encoding: str = 'Shift_JIS') -> Optional[str]:
        if not utils.is_valid_url(url):
            raise ValueError(f"Invalid URL: {url}")

        try:
            response = self.session.get(url, timeout=self.timeout)
            # HTTPステータスコードが正常でない場合はエラーを発生させる
            response.raise_for_status()

            # エンコーディングを設定
            response.encoding = encoding

            # リクエスト間の待機
            utils.sleep_with_jitter(self.request_delay)

            return response.text

        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch {url}: {e}") from e

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # ブロックを抜けたら自動でセッションをクローズする
        self.close()

