from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date, datetime, timedelta, timezone
from typing import List, Optional

from src.scraping.parser import PostInfo, ThreadInfo, parse_board_page, parse_thread_page
from src.scraping.scraper import Scraper
from src.scraping.utils import build_url


JST = timezone(timedelta(hours=9))


@dataclass
class CollectedPost:
    thread_path: str
    date: str
    content: str


def _get_target_date_jst(target_date: Optional[Date] = None) -> Date:
    if target_date is not None:
        return target_date

    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    now_jst = now_utc.astimezone(JST)
    return now_jst.date() - timedelta(days=1)


def _build_date_prefix(target_date: Date) -> str:
    return target_date.strftime("%Y/%m/%d(")


def collect_posts_for_date(
    base_url: str,
    board_path: str,
    target_date: Optional[Date] = None,
    *,
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    request_delay: float = 2.0,
    max_posts: Optional[int] = None,
) -> List[CollectedPost]:
    """
    指定した板トップページからスレッド一覧を取得し、
    「昨日（日本時間）に投稿されたレス」だけを収集する。

    処理フロー（memo/ロジック/日次データ収集.md に対応）:
      1. トップページをスクレイピングしスレッド一覧を取得
      2. 先頭のスレッドから順に、スレッドページを取得して投稿一覧を取得
      3. 昨日（日本時間）の投稿を抽出
      4. 「昨日の投稿が1件も存在しないスレッド」に到達したら、それ以降のスレッドは巡回せず終了

    Parameters
    ----------
    base_url : str
        例: "https://medaka.5ch.net"
    board_path : str
        板トップページのパス。
        例: "/test/read.cgi/prog/" （末尾スラッシュ有り/無しどちらでも可）
    target_date : datetime.date, optional
        収集対象の日付（日本時間）。省略時は「現在のJST日付 - 1日」を自動計算。
    timeout, max_retries, backoff_factor, request_delay :
        Scraper にそのまま渡すHTTP設定。
    max_posts : int, optional
        取得する最大投稿数。指定した場合、URLに/l{max_posts}を付けて最新の投稿のみを取得。
        例: max_posts=300 の場合、URLは /test/read.cgi/prog/1765368460/l300 となる。
        省略時は全件取得を試みる。

    Returns
    -------
    List[CollectedPost]
        スレッドパス・日付文字列・本文を含む投稿一覧。
    """
    target = _get_target_date_jst(target_date)
    date_prefix = _build_date_prefix(target)
    today = target + timedelta(days=1)
    today_prefix = _build_date_prefix(today)

    collected: List[CollectedPost] = []

    with Scraper(
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        request_delay=request_delay,
    ) as scraper:
        # 1. トップページ（板ページ）を取得
        board_url = build_url(base_url, board_path)
        board_html = scraper.fetch(board_url)

        if board_html is None:
            return collected

        threads: List[ThreadInfo] = parse_board_page(board_html)

        # 2. スレッドを新しい順に巡回
        for thread in threads:
            # max_postsが指定されている場合は、URLに/l{max_posts}を付ける
            if max_posts is not None:
                thread_path_with_limit = f"{thread.path}/l{max_posts}"
                thread_url = build_url(base_url, thread_path_with_limit)
            else:
                thread_url = build_url(base_url, thread.path)
            thread_html = scraper.fetch(thread_url)

            if thread_html is None:
                # このスレが取得できなかった場合はスキップして次へ
                continue

            posts: List[PostInfo] = parse_thread_page(thread_html)
            
            # デバッグ: 取得したHTMLのサイズと投稿数を確認
            # コンテナ内とコンテナ外で取得できるHTMLのサイズが異なる可能性がある
            import logging
            logger = logging.getLogger(__name__)
            logger.info(
                f"Thread {thread.path}: HTML size={len(thread_html)} chars, "
                f"Posts parsed={len(posts)}"
            )

            # 3. 昨日の日付に一致する投稿のみ抽出
            target_posts = [
                post for post in posts if post.date.startswith(date_prefix)
            ]

            # 今日の投稿もチェック
            today_posts = [
                post for post in posts if post.date.startswith(today_prefix)
            ]

            if not target_posts and not today_posts:
                # 4. 昨日の投稿が存在しないかつ今日の投稿が存在しないスレに到達したらループを終了
                break

            for post in target_posts:
                collected.append(
                    CollectedPost(
                        thread_path=thread.path,
                        date=post.date,
                        content=post.content,
                    )
                )

    return collected


