"""週次パイプラインの結合テスト"""
import pytest
from datetime import date, timedelta

from src.analysis.weekly_processor import WeeklyProcessor
from src.database.repositories import (
    PipelineRunRepository,
    DailyTermStatsRepository,
    WeeklyTermTrendsRepository,
    PipelineMetricsDailyRepository,
)
from src.database.models import (
    PipelineRun,
    Term,
    DailyTermStats,
    WeeklyTermTrends,
    PipelineMetricsDaily,
)


@pytest.mark.integration
class TestWeeklyPipeline:
    """週次パイプラインの結合テスト"""
    
    def test_end_to_end_weekly_processing(self, test_session):
        """週次統計計算のエンドツーエンドテスト"""
        board_key = "prog"
        
        # テストデータ準備: 1週間分の日次データを作成
        # 実行日を月曜日とする（weekday=0）
        execution_date = date.today()
        # 実行日を月曜日に調整
        days_since_monday = execution_date.weekday()
        execution_date = execution_date - timedelta(days=days_since_monday)
        
        # 先週の月曜日から日曜日まで
        week_start = execution_date - timedelta(days=7)
        week_end = week_start + timedelta(days=6)
        
        # リポジトリの初期化
        from src.database.repositories import TermRepository
        
        run_repo = PipelineRunRepository(test_session)
        term_repo = TermRepository(test_session)
        daily_stats_repo = DailyTermStatsRepository(test_session)
        metrics_repo = PipelineMetricsDailyRepository(test_session)
        
        # テスト用のTermを作成
        test_term = term_repo.get_or_create("テスト")
        test_session.flush()
        
        # 1週間分のDailyTermStatsとPipelineRun、PipelineMetricsDailyを作成
        current_date = week_start
        while current_date <= week_end:
            # パイプライン実行記録
            pipeline_run = PipelineRun(
                target_date=current_date,
                board_key=board_key,
                status="success",
                config={},
            )
            run_repo.create(pipeline_run)
            test_session.flush()
            
            # 日次統計（投稿数は増加傾向にする）
            day_index = (current_date - week_start).days
            daily_stat = DailyTermStats(
                date=current_date,
                board_key=board_key,
                term_id=test_term.term_id,
                post_hits=10 + day_index * 2,  # 10, 12, 14, ...
                thread_hits=1 + day_index,
            )
            daily_stats_repo.create(daily_stat)
            test_session.flush()
            
            # メトリクス（total_postsの計算に必要）
            metrics = PipelineMetricsDaily(
                date=current_date,
                board_key=board_key,
                run_id=pipeline_run.run_id,
                fetched_threads=10,
                fetched_posts=100 + day_index * 10,  # 100, 110, 120, ...
                parsed_posts=100 + day_index * 10,
                parse_fail_posts=0,
                tokenize_fail_posts=0,
                filtered_tokens=0,
                total_tokens=500,
                filtered_rate=0.0,
                duration_sec=60,
            )
            metrics_repo.upsert(metrics)
            test_session.flush()
            
            current_date += timedelta(days=1)
        
        test_session.commit()
        
        # 週次処理の実行
        processor = WeeklyProcessor(test_session)
        metrics = processor.process_weekly_analysis(
            execution_date=execution_date,
            board_key=board_key,
        )
        
        test_session.commit()
        
        # 検証: 週次トレンドが保存されているか
        weekly_trends_repo = WeeklyTermTrendsRepository(test_session)
        trends = weekly_trends_repo.get_by_week_and_board(
            week_start_date=week_start,
            board_key=board_key,
        )
        
        assert len(trends) > 0
        
        # テスト用のTermの週次トレンドが存在するか
        test_trend = next(
            (t for t in trends if t.term_id == test_term.term_id),
            None
        )
        assert test_trend is not None
        assert test_trend.post_hits > 0
        assert 0 <= test_trend.appearance_rate <= 1
        
        # 検証: 統計計算が正しく行われているか
        assert test_trend.appearance_rate_ci_lower is not None
        assert test_trend.appearance_rate_ci_upper is not None
        assert test_trend.appearance_rate_ci_lower <= test_trend.appearance_rate
        assert test_trend.appearance_rate <= test_trend.appearance_rate_ci_upper
        
        # 検証: メトリクスが正しく記録されているか
        assert metrics.processed_terms > 0
        assert metrics.duration_sec >= 0
    
    def test_weekly_processing_with_no_data(self, test_session):
        """データが存在しない場合の週次処理テスト"""
        board_key = "prog"
        execution_date = date.today()
        days_since_monday = execution_date.weekday()
        execution_date = execution_date - timedelta(days=days_since_monday)
        
        processor = WeeklyProcessor(test_session)
        metrics = processor.process_weekly_analysis(
            execution_date=execution_date,
            board_key=board_key,
        )
        
        test_session.commit()
        
        # 検証: データがなくてもエラーにならない
        assert metrics.processed_terms == 0
        assert len(metrics.invalid_dates) == 7  # 1週間分のデータがない

