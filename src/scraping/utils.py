import time
from typing import Optional
from urllib.parse import urljoin, urlparse


def build_url(base_url: str, path: str) -> str:
    result = urljoin(base_url, path)
    # 空のパスの場合、末尾スラッシュを追加
    if not path and not result.endswith('/'):
        result += '/'
    return result


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def get_default_headers() -> dict[str, str]:
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


def sleep_with_jitter(base_seconds: float, jitter_range: float = 0.5) -> None:
    # ランダムな待機時間でスリープする（レート制限対策）
    import random
    sleep_time = base_seconds + random.uniform(-jitter_range, jitter_range)
    time.sleep(max(0, sleep_time))


def get_excluded_thread_titles() -> list[str]:
    # プロジェクト共通の除外スレッドタイトルをここで管理する
    EXCLUDED_THREAD_TITLES: list[str] = [
        "★ UPLIFT プレミアム・サービスのお知らせ",
        "★ 5ちゃんねるから新しいお知らせです",
    ]
    return EXCLUDED_THREAD_TITLES


def extract_thread_id_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        # /test/read.cgi/prog/[ID]/ の形式から [ID] を抽出
        path_parts = [p for p in parsed.path.strip('/').split('/') if p]
        if 'read.cgi' in path_parts:
            read_cgi_index = path_parts.index('read.cgi')
            # read.cgiの次の次の要素がスレッドID（例: /test/read.cgi/prog/1764640243）
            if read_cgi_index + 2 < len(path_parts):
                thread_id = path_parts[read_cgi_index + 2]
                return thread_id
        return None
    except Exception:
        return None

