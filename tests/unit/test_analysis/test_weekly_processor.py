import pytest
from datetime import date, datetime, timedelta
from unittest.mock import Mock, MagicMock

from src.analysis.weekly_processor import (
    WeeklyProcessor,
    WeeklyProcessorMetrics,
)
from src.database.models import (
    PipelineRun,
    PipelineMetricsDaily,
    WeeklyTermTrends,
)


@pytest.fixture
def mock_session():
    """モックセッション"""
    session = Mock()
    session.add = Mock()
    session.flush = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.query = Mock()
    return session


@pytest.fixture
def weekly_processor(mock_session):
    """WeeklyProcessorインスタンス"""
    return WeeklyProcessor(mock_session)


class TestCalculateWeekRange:
    """calculate_week_rangeのテスト"""
    
    def test_monday_execution(self, weekly_processor):
        """月曜日の実行"""
        # 2024年1月8日（月曜日）を実行日とする
        execution_date = date(2024, 1, 8)
        
        week_start, week_end = weekly_processor.calculate_week_range(
            execution_date
        )
        
        # 先週の月曜日（2024年1月1日）と日曜日（2024年1月7日）になるはず
        assert week_start == date(2024, 1, 1)
        assert week_end == date(2024, 1, 7)
        assert week_start.weekday() == 0  # 月曜日
        assert week_end.weekday() == 6  # 日曜日
    
    def test_non_monday_execution(self, weekly_processor):
        """月曜日以外の実行（警告が出るが処理は継続）"""
        # 2024年1月9日（火曜日）を実行日とする
        execution_date = date(2024, 1, 9)
        
        week_start, week_end = weekly_processor.calculate_week_range(
            execution_date
        )
        
        # 7日前の月曜日（2024年1月1日）とその週の日曜日（2024年1月7日）になるはず
        assert week_start == date(2024, 1, 1)
        assert week_end == date(2024, 1, 7)


class TestValidateDataCollection:
    """validate_data_collectionのテスト"""
    
    def test_all_dates_valid(self, weekly_processor):
        """全ての日が有効な場合"""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        board_key = "prog"
        
        # モックの設定
        def mock_get_by_date_and_board(target_date, board_key):
            run = Mock(spec=PipelineRun)
            run.status = 'success'
            run.is_recovered = False
            return run
        
        weekly_processor.pipeline_run_repo.get_by_date_and_board = (
            mock_get_by_date_and_board
        )
        
        valid_dates = weekly_processor.validate_data_collection(
            start_date,
            end_date,
            board_key,
        )
        
        assert len(valid_dates) == 7
        assert start_date in valid_dates
        assert end_date in valid_dates
    
    def test_some_dates_invalid(self, weekly_processor):
        """一部の日が無効な場合"""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        board_key = "prog"
        
        # モックの設定（1月3日と1月5日が無効）
        def mock_get_by_date_and_board(target_date, board_key):
            if target_date in [date(2024, 1, 3), date(2024, 1, 5)]:
                run = Mock(spec=PipelineRun)
                run.status = 'failed'
                run.is_recovered = False
                return run
            else:
                run = Mock(spec=PipelineRun)
                run.status = 'success'
                run.is_recovered = False
                return run
        
        weekly_processor.pipeline_run_repo.get_by_date_and_board = (
            mock_get_by_date_and_board
        )
        
        valid_dates = weekly_processor.validate_data_collection(
            start_date,
            end_date,
            board_key,
        )
        
        assert len(valid_dates) == 5
        assert date(2024, 1, 3) not in valid_dates
        assert date(2024, 1, 5) not in valid_dates
    
    def test_recovered_run(self, weekly_processor):
        """is_recoveredがtrueの場合も有効"""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        board_key = "prog"
        
        # モックの設定
        def mock_get_by_date_and_board(target_date, board_key):
            run = Mock(spec=PipelineRun)
            run.status = 'failed'
            run.is_recovered = True  # リカバリー済み
            return run
        
        weekly_processor.pipeline_run_repo.get_by_date_and_board = (
            mock_get_by_date_and_board
        )
        
        valid_dates = weekly_processor.validate_data_collection(
            start_date,
            end_date,
            board_key,
        )
        
        assert len(valid_dates) == 7
    
    def test_no_pipeline_run(self, weekly_processor):
        """pipeline_runが存在しない場合"""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        board_key = "prog"
        
        # モックの設定（常にNoneを返す）
        weekly_processor.pipeline_run_repo.get_by_date_and_board = (
            Mock(return_value=None)
        )
        
        valid_dates = weekly_processor.validate_data_collection(
            start_date,
            end_date,
            board_key,
        )
        
        assert len(valid_dates) == 0


class TestProcessWeeklyAnalysis:
    """process_weekly_analysisのテスト"""
    
    def test_basic_flow(self, weekly_processor):
        """基本的な処理フロー"""
        execution_date = date(2024, 1, 8)  # 月曜日
        board_key = "prog"
        
        # モックの設定
        week_start = date(2024, 1, 1)
        week_end = date(2024, 1, 7)
        
        # calculate_week_rangeのモック
        weekly_processor.calculate_week_range = Mock(
            return_value=(week_start, week_end)
        )
        
        # validate_data_collectionのモック
        valid_dates = {week_start + timedelta(days=i) for i in range(7)}
        weekly_processor.validate_data_collection = Mock(
            return_value=valid_dates
        )
        
        # get_weekly_aggregationのモック
        weekly_processor.daily_stats_repo.get_weekly_aggregation = Mock(
            return_value=[
                {'term_id': 1, 'post_hits': 10, 'thread_hits': 5},
                {'term_id': 2, 'post_hits': 20, 'thread_hits': 8},
            ]
        )
        
        # get_by_date_and_boardのモック（total_posts用）
        def mock_get_by_date_and_board(target_date, board_key):
            metrics = Mock(spec=PipelineMetricsDaily)
            metrics.fetched_posts = 100
            return metrics
        
        weekly_processor.metrics_repo.get_by_date_and_board = (
            mock_get_by_date_and_board
        )
        
        # get_by_term_and_week_rangeのモック（z-score用、過去データなし）
        weekly_processor.weekly_trends_repo.get_by_term_and_week_range = (
            Mock(return_value=[])
        )
        
        # upsertのモック
        weekly_processor.weekly_trends_repo.upsert = Mock()
        weekly_processor.regression_repo.upsert = Mock()
        
        # 実行
        metrics = weekly_processor.process_weekly_analysis(
            execution_date,
            board_key,
        )
        
        # 検証
        assert metrics.processed_terms == 2
        assert metrics.error_terms == 0
        assert weekly_processor.weekly_trends_repo.upsert.call_count == 2
    
    def test_no_valid_dates(self, weekly_processor):
        """有効な日が存在しない場合"""
        execution_date = date(2024, 1, 8)
        board_key = "prog"
        
        week_start = date(2024, 1, 1)
        week_end = date(2024, 1, 7)
        
        weekly_processor.calculate_week_range = Mock(
            return_value=(week_start, week_end)
        )
        weekly_processor.validate_data_collection = Mock(return_value=set())
        
        metrics = weekly_processor.process_weekly_analysis(
            execution_date,
            board_key,
        )
        
        assert metrics.processed_terms == 0
        assert len(metrics.invalid_dates) == 7


class TestWeeklyProcessorMetrics:
    """WeeklyProcessorMetricsのテスト"""
    
    def test_duration_sec(self):
        """処理時間の計算"""
        metrics = WeeklyProcessorMetrics()
        metrics.start_time = datetime(2024, 1, 1, 10, 0, 0)
        metrics.end_time = datetime(2024, 1, 1, 10, 0, 30)
        
        assert metrics.duration_sec == 30
    
    def test_duration_sec_no_times(self):
        """開始時刻・終了時刻が設定されていない場合"""
        metrics = WeeklyProcessorMetrics()
        
        assert metrics.duration_sec == 0

