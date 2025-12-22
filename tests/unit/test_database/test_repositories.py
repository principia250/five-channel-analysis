"""リポジトリクラスのテスト"""
import pytest
from datetime import date
from unittest.mock import Mock, MagicMock
from uuid import uuid4, UUID

from src.database.repositories import (
    PipelineRunRepository,
    TermRepository,
    DailyTermStatsRepository,
    WeeklyTermTrendsRepository,
    TermRegressionResultRepository,
    PipelineMetricsDailyRepository,
)
from src.database.models import (
    PipelineRun,
    Term,
    DailyTermStats,
    WeeklyTermTrends,
    TermRegressionResult,
    PipelineMetricsDaily,
)


@pytest.fixture
def mock_session():
    """モックセッション"""
    session = Mock()
    session.add = Mock()
    session.flush = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    return session


@pytest.fixture
def mock_query():
    """モッククエリ"""
    query = Mock()
    query.filter = Mock(return_value=query)
    query.order_by = Mock(return_value=query)
    query.limit = Mock(return_value=query)
    query.offset = Mock(return_value=query)
    query.first = Mock(return_value=None)
    query.all = Mock(return_value=[])
    return query


class TestPipelineRunRepository:
    """PipelineRunRepositoryのテスト"""

    def test_create(self, mock_session):
        """パイプライン実行履歴を作成できる"""
        repo = PipelineRunRepository(mock_session)
        pipeline_run = PipelineRun(
            target_date=date(2025, 1, 1),
            board_key="prog",
            status="success",
            config={}
        )
        
        result = repo.create(pipeline_run)
        
        assert result == pipeline_run
        mock_session.add.assert_called_once_with(pipeline_run)
        mock_session.flush.assert_called_once()

    def test_get_by_id_found(self, mock_session, mock_query):
        """IDで取得できる（見つかった場合）"""
        repo = PipelineRunRepository(mock_session)
        run_id = uuid4()
        expected_run = PipelineRun(
            run_id=run_id,
            target_date=date(2025, 1, 1),
            board_key="prog",
            status="success",
            config={}
        )
        mock_query.first.return_value = expected_run
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_id(run_id)
        
        assert result == expected_run
        mock_session.query.assert_called_once_with(PipelineRun)

    def test_get_by_id_not_found(self, mock_session, mock_query):
        """IDで取得できない（見つからなかった場合）"""
        repo = PipelineRunRepository(mock_session)
        run_id = uuid4()
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_id(run_id)
        
        assert result is None

    def test_get_by_date_and_board(self, mock_session, mock_query):
        """日付とボードキーで取得できる"""
        repo = PipelineRunRepository(mock_session)
        target_date = date(2025, 1, 1)
        board_key = "prog"
        expected_run = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={}
        )
        mock_query.first.return_value = expected_run
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_date_and_board(target_date, board_key)
        
        assert result == expected_run
        mock_session.query.assert_called_once_with(PipelineRun)

    def test_update_status(self, mock_session, mock_query):
        """ステータスを更新できる"""
        repo = PipelineRunRepository(mock_session)
        run_id = uuid4()
        pipeline_run = PipelineRun(
            run_id=run_id,
            target_date=date(2025, 1, 1),
            board_key="prog",
            status="running",
            config={}
        )
        mock_query.first.return_value = pipeline_run
        mock_session.query.return_value = mock_query
        
        result = repo.update_status(run_id, "success", date(2025, 1, 2))
        
        assert result == pipeline_run
        assert pipeline_run.status == "success"
        assert pipeline_run.finished_at == date(2025, 1, 2)
        mock_session.flush.assert_called_once()

    def test_update_status_not_found(self, mock_session, mock_query):
        """存在しないIDで更新しようとした場合"""
        repo = PipelineRunRepository(mock_session)
        run_id = uuid4()
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.update_status(run_id, "success")
        
        assert result is None
        mock_session.flush.assert_not_called()


class TestTermRepository:
    """TermRepositoryのテスト"""

    def test_create(self, mock_session):
        """名詞を作成できる"""
        repo = TermRepository(mock_session)
        term = Term(normalized="Python")
        
        result = repo.create(term)
        
        assert result == term
        mock_session.add.assert_called_once_with(term)
        mock_session.flush.assert_called_once()

    def test_get_by_id(self, mock_session, mock_query):
        """IDで取得できる"""
        repo = TermRepository(mock_session)
        term_id = 1
        expected_term = Term(term_id=term_id, normalized="Python")
        mock_query.first.return_value = expected_term
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_id(term_id)
        
        assert result == expected_term

    def test_get_by_normalized(self, mock_session, mock_query):
        """正規化された名詞で取得できる"""
        repo = TermRepository(mock_session)
        normalized = "Python"
        expected_term = Term(normalized=normalized)
        mock_query.first.return_value = expected_term
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_normalized(normalized)
        
        assert result == expected_term

    def test_get_or_create_existing(self, mock_session, mock_query):
        """既存の名詞を取得できる"""
        repo = TermRepository(mock_session)
        normalized = "Python"
        existing_term = Term(term_id=1, normalized=normalized)
        mock_query.first.return_value = existing_term
        mock_session.query.return_value = mock_query
        
        result = repo.get_or_create(normalized)
        
        assert result == existing_term
        mock_session.add.assert_not_called()

    def test_get_or_create_new(self, mock_session, mock_query):
        """新しい名詞を作成できる"""
        repo = TermRepository(mock_session)
        normalized = "Python"
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.get_or_create(normalized)
        
        assert result.normalized == normalized
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()

    def test_update_blocked(self, mock_session, mock_query):
        """ブロック状態を更新できる"""
        repo = TermRepository(mock_session)
        term_id = 1
        term = Term(term_id=term_id, normalized="Python", is_blocked=False)
        mock_query.first.return_value = term
        mock_session.query.return_value = mock_query
        
        result = repo.update_blocked(term_id, True, "spam")
        
        assert result == term
        assert term.is_blocked is True
        assert term.blocked_reason == "spam"
        mock_session.flush.assert_called_once()


class TestDailyTermStatsRepository:
    """DailyTermStatsRepositoryのテスト"""

    def test_create(self, mock_session):
        """日次統計を作成できる"""
        repo = DailyTermStatsRepository(mock_session)
        stats = DailyTermStats(
            date=date(2025, 1, 1),
            board_key="prog",
            term_id=1,
            post_hits=10,
            thread_hits=5
        )
        
        result = repo.create(stats)
        
        assert result == stats
        mock_session.add.assert_called_once_with(stats)
        mock_session.flush.assert_called_once()

    def test_get_by_date_and_board(self, mock_session, mock_query):
        """日付とボードキーで取得できる"""
        repo = DailyTermStatsRepository(mock_session)
        target_date = date(2025, 1, 1)
        board_key = "prog"
        expected_stats = [
            DailyTermStats(
                date=target_date,
                board_key=board_key,
                term_id=1,
                post_hits=10,
                thread_hits=5
            )
        ]
        mock_query.all.return_value = expected_stats
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_date_and_board(target_date, board_key)
        
        assert result == expected_stats
        mock_query.order_by.assert_called_once()

    def test_get_by_date_and_board_with_limit(self, mock_session, mock_query):
        """limitを指定して取得できる"""
        repo = DailyTermStatsRepository(mock_session)
        target_date = date(2025, 1, 1)
        board_key = "prog"
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query
        
        repo.get_by_date_and_board(target_date, board_key, limit=10, offset=5)
        
        mock_query.limit.assert_called_once_with(10)
        mock_query.offset.assert_called_once_with(5)

    def test_upsert_existing(self, mock_session, mock_query):
        """既存の統計を更新できる"""
        repo = DailyTermStatsRepository(mock_session)
        existing_stats = DailyTermStats(
            date=date(2025, 1, 1),
            board_key="prog",
            term_id=1,
            post_hits=10,
            thread_hits=5
        )
        new_stats = DailyTermStats(
            date=date(2025, 1, 1),
            board_key="prog",
            term_id=1,
            post_hits=20,
            thread_hits=10
        )
        mock_query.first.return_value = existing_stats
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_stats)
        
        assert result == existing_stats
        assert existing_stats.post_hits == 20
        assert existing_stats.thread_hits == 10
        mock_session.flush.assert_called_once()
        mock_session.add.assert_not_called()

    def test_upsert_new(self, mock_session, mock_query):
        """新しい統計を作成できる"""
        repo = DailyTermStatsRepository(mock_session)
        new_stats = DailyTermStats(
            date=date(2025, 1, 1),
            board_key="prog",
            term_id=1,
            post_hits=10,
            thread_hits=5
        )
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_stats)
        
        assert result == new_stats
        mock_session.add.assert_called_once_with(new_stats)
        mock_session.flush.assert_called()


class TestWeeklyTermTrendsRepository:
    """WeeklyTermTrendsRepositoryのテスト"""

    def test_create(self, mock_session):
        """週次トレンドを作成できる"""
        repo = WeeklyTermTrendsRepository(mock_session)
        trend = WeeklyTermTrends(
            week_start_date=date(2025, 1, 6),
            board_key="prog",
            term_id=1,
            post_hits=100,
            total_posts=1000,
            appearance_rate=0.1
        )
        
        result = repo.create(trend)
        
        assert result == trend
        mock_session.add.assert_called_once_with(trend)
        mock_session.flush.assert_called_once()

    def test_get_by_week_and_board(self, mock_session, mock_query):
        """週とボードキーで取得できる"""
        repo = WeeklyTermTrendsRepository(mock_session)
        week_start_date = date(2025, 1, 6)
        board_key = "prog"
        expected_trends = [
            WeeklyTermTrends(
                week_start_date=week_start_date,
                board_key=board_key,
                term_id=1,
                post_hits=100,
                total_posts=1000,
                appearance_rate=0.1,
                zscore=2.5
            )
        ]
        mock_query.all.return_value = expected_trends
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_week_and_board(week_start_date, board_key)
        
        assert result == expected_trends

    def test_get_by_week_and_board_with_limit(self, mock_session, mock_query):
        """limitを指定して取得できる"""
        repo = WeeklyTermTrendsRepository(mock_session)
        week_start_date = date(2025, 1, 6)
        board_key = "prog"
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query
        
        repo.get_by_week_and_board(week_start_date, board_key, limit=10)
        
        mock_query.limit.assert_called_once_with(10)

    def test_upsert_existing(self, mock_session, mock_query):
        """既存のトレンドを更新できる"""
        repo = WeeklyTermTrendsRepository(mock_session)
        existing_trend = WeeklyTermTrends(
            week_start_date=date(2025, 1, 6),
            board_key="prog",
            term_id=1,
            post_hits=100,
            total_posts=1000,
            appearance_rate=0.1,
            zscore=2.0
        )
        new_trend = WeeklyTermTrends(
            week_start_date=date(2025, 1, 6),
            board_key="prog",
            term_id=1,
            post_hits=200,
            total_posts=2000,
            appearance_rate=0.2,
            zscore=3.0
        )
        mock_query.first.return_value = existing_trend
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_trend)
        
        assert result == existing_trend
        assert existing_trend.post_hits == 200
        assert existing_trend.zscore == 3.0
        mock_session.flush.assert_called_once()

    def test_upsert_new(self, mock_session, mock_query):
        """新しいトレンドを作成できる"""
        repo = WeeklyTermTrendsRepository(mock_session)
        new_trend = WeeklyTermTrends(
            week_start_date=date(2025, 1, 6),
            board_key="prog",
            term_id=1,
            post_hits=100,
            total_posts=1000,
            appearance_rate=0.1
        )
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_trend)
        
        assert result == new_trend
        mock_session.add.assert_called_once_with(new_trend)


class TestTermRegressionResultRepository:
    """TermRegressionResultRepositoryのテスト"""

    def test_create(self, mock_session):
        """回帰分析結果を作成できる"""
        repo = TermRegressionResultRepository(mock_session)
        result = TermRegressionResult(
            board_key="prog",
            term_id=1,
            intercept=0.5,
            slope=0.1,
            analysis_start_date=date(2025, 1, 1),
            analysis_end_date=date(2025, 1, 31)
        )
        
        created = repo.create(result)
        
        assert created == result
        mock_session.add.assert_called_once_with(result)
        mock_session.flush.assert_called_once()

    def test_get_by_board_and_term(self, mock_session, mock_query):
        """ボードキーと名詞IDで取得できる"""
        repo = TermRegressionResultRepository(mock_session)
        board_key = "prog"
        term_id = 1
        expected_result = TermRegressionResult(
            board_key=board_key,
            term_id=term_id,
            intercept=0.5,
            slope=0.1,
            analysis_start_date=date(2025, 1, 1),
            analysis_end_date=date(2025, 1, 31)
        )
        mock_query.first.return_value = expected_result
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_board_and_term(board_key, term_id)
        
        assert result == expected_result

    def test_get_by_board_sorted_by_slope(self, mock_session, mock_query):
        """ボードキーで取得できる（slope順）"""
        repo = TermRegressionResultRepository(mock_session)
        board_key = "prog"
        expected_results = [
            TermRegressionResult(
                board_key=board_key,
                term_id=1,
                intercept=0.5,
                slope=0.2,
                analysis_start_date=date(2025, 1, 1),
                analysis_end_date=date(2025, 1, 31)
            )
        ]
        mock_query.all.return_value = expected_results
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_board_sorted_by_slope(board_key)
        
        assert result == expected_results
        mock_query.order_by.assert_called_once()

    def test_get_by_board_sorted_by_slope_with_limit(self, mock_session, mock_query):
        """limitを指定して取得できる"""
        repo = TermRegressionResultRepository(mock_session)
        board_key = "prog"
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query
        
        repo.get_by_board_sorted_by_slope(board_key, limit=10)
        
        mock_query.limit.assert_called_once_with(10)

    def test_upsert_existing(self, mock_session, mock_query):
        """既存の結果を更新できる"""
        repo = TermRegressionResultRepository(mock_session)
        existing_result = TermRegressionResult(
            board_key="prog",
            term_id=1,
            intercept=0.5,
            slope=0.1,
            analysis_start_date=date(2025, 1, 1),
            analysis_end_date=date(2025, 1, 31)
        )
        new_result = TermRegressionResult(
            board_key="prog",
            term_id=1,
            intercept=0.6,
            slope=0.2,
            p_value=0.01,
            analysis_start_date=date(2025, 1, 1),
            analysis_end_date=date(2025, 2, 1)
        )
        mock_query.first.return_value = existing_result
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_result)
        
        assert result == existing_result
        assert existing_result.slope == 0.2
        assert existing_result.p_value == 0.01
        mock_session.flush.assert_called_once()

    def test_upsert_new(self, mock_session, mock_query):
        """新しい結果を作成できる"""
        repo = TermRegressionResultRepository(mock_session)
        new_result = TermRegressionResult(
            board_key="prog",
            term_id=1,
            intercept=0.5,
            slope=0.1,
            analysis_start_date=date(2025, 1, 1),
            analysis_end_date=date(2025, 1, 31)
        )
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_result)
        
        assert result == new_result
        mock_session.add.assert_called_once_with(new_result)


class TestPipelineMetricsDailyRepository:
    """PipelineMetricsDailyRepositoryのテスト"""

    def test_create(self, mock_session):
        """パイプラインメトリクスを作成できる"""
        repo = PipelineMetricsDailyRepository(mock_session)
        metrics = PipelineMetricsDaily(
            date=date(2025, 1, 1),
            board_key="prog",
            fetched_threads=100,
            fetched_posts=1000,
            parsed_posts=950,
            parse_fail_posts=50,
            tokenize_fail_posts=10,
            filtered_tokens=5000,
            total_tokens=10000,
            filtered_rate=0.5,
            duration_sec=60
        )
        
        result = repo.create(metrics)
        
        assert result == metrics
        mock_session.add.assert_called_once_with(metrics)
        mock_session.flush.assert_called_once()

    def test_get_by_date_and_board(self, mock_session, mock_query):
        """日付とボードキーで取得できる"""
        repo = PipelineMetricsDailyRepository(mock_session)
        target_date = date(2025, 1, 1)
        board_key = "prog"
        expected_metrics = PipelineMetricsDaily(
            date=target_date,
            board_key=board_key,
            fetched_threads=100,
            fetched_posts=1000,
            parsed_posts=950,
            parse_fail_posts=50,
            tokenize_fail_posts=10,
            filtered_tokens=5000,
            total_tokens=10000,
            filtered_rate=0.5,
            duration_sec=60
        )
        mock_query.first.return_value = expected_metrics
        mock_session.query.return_value = mock_query
        
        result = repo.get_by_date_and_board(target_date, board_key)
        
        assert result == expected_metrics

    def test_upsert_existing(self, mock_session, mock_query):
        """既存のメトリクスを更新できる"""
        repo = PipelineMetricsDailyRepository(mock_session)
        existing_metrics = PipelineMetricsDaily(
            date=date(2025, 1, 1),
            board_key="prog",
            fetched_threads=100,
            fetched_posts=1000,
            parsed_posts=950,
            parse_fail_posts=50,
            tokenize_fail_posts=10,
            filtered_tokens=5000,
            total_tokens=10000,
            filtered_rate=0.5,
            duration_sec=60
        )
        new_metrics = PipelineMetricsDaily(
            date=date(2025, 1, 1),
            board_key="prog",
            fetched_threads=200,
            fetched_posts=2000,
            parsed_posts=1900,
            parse_fail_posts=100,
            tokenize_fail_posts=20,
            filtered_tokens=10000,
            total_tokens=20000,
            filtered_rate=0.5,
            duration_sec=120
        )
        mock_query.first.return_value = existing_metrics
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_metrics)
        
        assert result == existing_metrics
        assert existing_metrics.fetched_threads == 200
        assert existing_metrics.duration_sec == 120
        mock_session.flush.assert_called_once()
        mock_session.add.assert_not_called()

    def test_upsert_new(self, mock_session, mock_query):
        """新しいメトリクスを作成できる"""
        repo = PipelineMetricsDailyRepository(mock_session)
        new_metrics = PipelineMetricsDaily(
            date=date(2025, 1, 1),
            board_key="prog",
            fetched_threads=100,
            fetched_posts=1000,
            parsed_posts=950,
            parse_fail_posts=50,
            tokenize_fail_posts=10,
            filtered_tokens=5000,
            total_tokens=10000,
            filtered_rate=0.5,
            duration_sec=60
        )
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        
        result = repo.upsert(new_metrics)
        
        assert result == new_metrics
        mock_session.add.assert_called_once_with(new_metrics)

