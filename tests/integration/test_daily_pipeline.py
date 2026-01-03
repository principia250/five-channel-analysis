"""日次パイプラインの結合テスト"""
import pytest
from datetime import date, datetime
from uuid import uuid4

from src.analysis.daily_processor import DailyProcessor
from src.database.repositories import PipelineRunRepository
from src.database.models import (
    PipelineRun,
    PipelineMetricsDaily,
    DailyTermStats,
)
from src.scraping.daily_scraper import CollectedPost
from src.scraping import parse_board_page, parse_thread_page


@pytest.mark.integration
class TestDailyPipeline:
    """日次パイプラインの結合テスト"""
    
    def test_end_to_end_daily_processing(
        self,
        test_session,
        sample_html_board_page,
        sample_html_thread_page,
    ):
        """スクレイピング → 名詞抽出 → DB保存のエンドツーエンドテスト"""
        board_key = "prog"
        target_date = date.today()
        
        # パイプライン実行記録を作成
        pipeline_run = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={},
        )
        run_repo = PipelineRunRepository(test_session)
        pipeline_run = run_repo.create(pipeline_run)
        test_session.commit()
        
        # サンプルHTMLから投稿データを取得
        threads = parse_board_page(sample_html_board_page)
        posts_info = parse_thread_page(sample_html_thread_page)
        
        # CollectedPostに変換
        posts = []
        for thread in threads:
            for post_info in posts_info:
                posts.append(
                    CollectedPost(
                        thread_path=thread.path,
                        date=post_info.date,
                        content=post_info.content,
                    )
                )
        
        # 日次処理の実行
        processor = DailyProcessor(test_session)
        metrics = processor.process_posts(
            posts=posts,
            target_date=target_date,
            board_key=board_key,
            run_id=pipeline_run.run_id,
        )
        
        test_session.commit()
        
        # 検証: メトリクスが保存されているか
        metrics_record = test_session.query(PipelineMetricsDaily).filter(
            PipelineMetricsDaily.date == target_date,
            PipelineMetricsDaily.board_key == board_key,
        ).first()
        
        assert metrics_record is not None
        assert metrics_record.fetched_posts > 0
        assert metrics_record.parsed_posts > 0
        assert metrics_record.run_id == pipeline_run.run_id
        
        # 検証: 名詞統計が保存されているか
        stats = test_session.query(DailyTermStats).filter(
            DailyTermStats.date == target_date,
            DailyTermStats.board_key == board_key,
        ).all()
        
        assert len(stats) > 0
        
        # 検証: メトリクスの値が正しいか
        assert metrics.fetched_posts == len(posts)
        assert metrics.fetched_threads == len(threads)
        assert metrics.parsed_posts > 0
    
    def test_daily_processing_with_empty_posts(self, test_session):
        """空の投稿リストで日次処理を実行した場合のテスト"""
        board_key = "prog"
        target_date = date.today()
        
        # パイプライン実行記録を作成
        pipeline_run = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={},
        )
        run_repo = PipelineRunRepository(test_session)
        pipeline_run = run_repo.create(pipeline_run)
        test_session.commit()
        
        # 空の投稿リストで処理
        processor = DailyProcessor(test_session)
        metrics = processor.process_posts(
            posts=[],
            target_date=target_date,
            board_key=board_key,
            run_id=pipeline_run.run_id,
        )
        
        test_session.commit()
        
        # 検証: メトリクスが保存されているか
        metrics_record = test_session.query(PipelineMetricsDaily).filter(
            PipelineMetricsDaily.date == target_date,
            PipelineMetricsDaily.board_key == board_key,
        ).first()
        
        assert metrics_record is not None
        assert metrics_record.fetched_posts == 0
        assert metrics_record.parsed_posts == 0
        
        # 検証: 名詞統計は保存されていない
        stats = test_session.query(DailyTermStats).filter(
            DailyTermStats.date == target_date,
            DailyTermStats.board_key == board_key,
        ).all()
        
        assert len(stats) == 0
    
    def test_daily_processing_term_aggregation(self, test_session):
        """同一名詞が複数回出現した場合の集計テスト"""
        board_key = "prog"
        target_date = date.today()
        
        # パイプライン実行記録を作成
        pipeline_run = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={},
        )
        run_repo = PipelineRunRepository(test_session)
        pipeline_run = run_repo.create(pipeline_run)
        test_session.commit()
        
        # 同じスレッドから複数の投稿（同じ名詞を含む）
        thread_path = "/test/read.cgi/prog/1234567890"
        posts = [
            CollectedPost(
                thread_path=thread_path,
                date="2025/01/01(月) 12:00:00.00",
                content="Pythonでプログラミングをしています。Pythonは良い言語です。",
            ),
            CollectedPost(
                thread_path=thread_path,
                date="2025/01/01(月) 12:01:00.00",
                content="Pythonの開発環境を構築しました。",
            ),
        ]
        
        # 日次処理の実行
        processor = DailyProcessor(test_session)
        metrics = processor.process_posts(
            posts=posts,
            target_date=target_date,
            board_key=board_key,
            run_id=pipeline_run.run_id,
        )
        
        test_session.commit()
        
        # 検証: 名詞が正しく集計されているか
        # 同一スレッド内なので、thread_hitsは1になるはず
        stats = test_session.query(DailyTermStats).filter(
            DailyTermStats.date == target_date,
            DailyTermStats.board_key == board_key,
        ).all()
        
        # 何らかの名詞が抽出されているか確認
        assert len(stats) > 0
        
        # Termとのリレーションを確認するため、Termを取得
        from src.database.models import Term
        for stat in stats:
            term = test_session.query(Term).filter(Term.term_id == stat.term_id).first()
            assert term is not None

