from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from src.analysis.noun_extractor import NounExtractor
from src.analysis.normalizer import normalize_term
from src.database.models import DailyTermStats, PipelineMetricsDaily, Term
from src.database.repositories import (
    DailyTermStatsRepository,
    PipelineMetricsDailyRepository,
    TermRepository,
)
from src.scraping.daily_scraper import CollectedPost


class DailyProcessorMetrics:
    """日次処理の品質メトリクスを保持するクラス"""
    
    def __init__(self):
        self.fetched_threads = 0
        self.fetched_posts = 0
        self.parsed_posts = 0
        self.parse_fail_posts = 0
        self.tokenize_fail_posts = 0
        self.total_tokens = 0
        self.filtered_tokens = 0
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    @property
    def duration_sec(self) -> int:
        """処理時間（秒）"""
        if self.start_time and self.end_time:
            return int((self.end_time - self.start_time).total_seconds())
        return 0
    
    @property
    def filtered_rate(self) -> float:
        """フィルタ率"""
        if self.total_tokens == 0:
            return 0.0
        return self.filtered_tokens / self.total_tokens


class DailyProcessor:
    """日次データ処理クラス：名詞抽出→正規化→DB保存"""
    
    def __init__(self, session: Session):
        self.session = session
        self.noun_extractor = NounExtractor()
        self.term_repo = TermRepository(session)
        self.daily_stats_repo = DailyTermStatsRepository(session)
        self.metrics_repo = PipelineMetricsDailyRepository(session)
    
    def process_posts(
        self,
        posts: List[CollectedPost],
        target_date: date,
        board_key: str,
        run_id: Optional[UUID] = None,
    ) -> DailyProcessorMetrics:
        """
        投稿リストを処理して名詞を抽出し、DBに保存する。
        
        Parameters
        ----------
        posts : List[CollectedPost]
            処理対象の投稿リスト
        target_date : date
            対象日付
        board_key : str
            板キー（例: "prog"）
        run_id : UUID, optional
            パイプライン実行ID
        
        Returns
        -------
        DailyProcessorMetrics
            処理メトリクス
        """
        metrics = DailyProcessorMetrics()
        metrics.start_time = datetime.now()
        
        # スレッドごとの投稿を集計
        thread_posts: Dict[str, List[CollectedPost]] = defaultdict(list)
        for post in posts:
            thread_posts[post.thread_path].append(post)
        
        metrics.fetched_threads = len(thread_posts)
        metrics.fetched_posts = len(posts)
        
        # 名詞の集計（term_id -> (post_hits, thread_hits)）
        # post_hits: その語を含んだレス数（同一レス内で複数回出ても1カウント）
        # thread_hits: その語を含んだスレ数（同一スレ内で複数レスに出ても1カウント）
        term_stats: Dict[int, Dict[str, int]] = defaultdict(
            lambda: {"post_hits": 0, "thread_hits": 0}
        )
        
        # スレッドごとに処理
        for thread_path, thread_post_list in thread_posts.items():
            thread_term_ids: set[int] = set()  # このスレッドで出現したterm_idのセット
            
            # 各投稿を処理
            for post in thread_post_list:
                metrics.parsed_posts += 1
                
                try:
                    # 名詞を抽出
                    nouns = self.noun_extractor.extract_nouns(post.content)
                    
                    if not nouns:
                        # 名詞が抽出できなかった場合（空の投稿など）
                        # これは失敗ではなく、単に名詞が含まれていないだけなのでカウントしない
                        continue
                    
                    # 投稿内で出現したterm_idのセット（同一レス内で複数回出ても1カウント）
                    post_term_ids: set[int] = set()
                    
                    for noun in nouns:
                        metrics.total_tokens += 1
                        
                        # 正規化
                        normalized = normalize_term(noun)
                        
                        if not normalized:
                            # 正規化後に空になった場合はフィルタ対象
                            metrics.filtered_tokens += 1
                            continue
                        
                        # Termを取得または作成
                        term = self.term_repo.get_or_create(normalized)
                        
                        # ブロックされている場合はスキップ
                        if term.is_blocked:
                            metrics.filtered_tokens += 1
                            continue
                        
                        term_id = term.term_id
                        
                        # この投稿で初めて出現したterm_idの場合のみカウント
                        if term_id not in post_term_ids:
                            post_term_ids.add(term_id)
                            term_stats[term_id]["post_hits"] += 1
                        
                        # このスレッドで初めて出現したterm_idの場合のみカウント
                        if term_id not in thread_term_ids:
                            thread_term_ids.add(term_id)
                            term_stats[term_id]["thread_hits"] += 1
                
                except Exception:
                    # トークン化に失敗した場合（Janomeのエラーなど）
                    metrics.tokenize_fail_posts += 1
                    continue
        
        # daily_term_statsに保存
        for term_id, stats in term_stats.items():
            daily_stats = DailyTermStats(
                date=target_date,
                board_key=board_key,
                term_id=term_id,
                post_hits=stats["post_hits"],
                thread_hits=stats["thread_hits"],
            )
            self.daily_stats_repo.upsert(daily_stats)
        
        metrics.end_time = datetime.now()
        
        # メトリクスを保存
        pipeline_metrics = PipelineMetricsDaily(
            date=target_date,
            board_key=board_key,
            run_id=run_id,
            fetched_threads=metrics.fetched_threads,
            fetched_posts=metrics.fetched_posts,
            parsed_posts=metrics.parsed_posts,
            parse_fail_posts=metrics.parse_fail_posts,
            tokenize_fail_posts=metrics.tokenize_fail_posts,
            total_tokens=metrics.total_tokens,
            filtered_tokens=metrics.filtered_tokens,
            filtered_rate=metrics.filtered_rate,
            duration_sec=metrics.duration_sec,
        )
        self.metrics_repo.upsert(pipeline_metrics)
        
        return metrics

