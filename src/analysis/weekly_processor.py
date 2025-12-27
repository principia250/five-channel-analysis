import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from src.analysis.statistics import (
    calculate_appearance_rate_ci,
    calculate_zscore,
    perform_linear_regression,
)
from src.database.models import (
    PipelineRun,
    TermRegressionResult,
    WeeklyTermTrends,
)
from src.database.repositories import (
    DailyTermStatsRepository,
    PipelineMetricsDailyRepository,
    PipelineRunRepository,
    TermRegressionResultRepository,
    WeeklyTermTrendsRepository,
)

logger = logging.getLogger(__name__)


class WeeklyProcessorMetrics:
    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.processed_terms = 0
        self.error_terms = 0
        self.invalid_dates: list[date] = []
    
    @property
    def duration_sec(self) -> int:
        if self.start_time and self.end_time:
            return int((self.end_time - self.start_time).total_seconds())
        return 0


class WeeklyProcessor:
    def __init__(self, session: Session):
        self.session = session
        self.pipeline_run_repo = PipelineRunRepository(session)
        self.daily_stats_repo = DailyTermStatsRepository(session)
        self.metrics_repo = PipelineMetricsDailyRepository(session)
        self.weekly_trends_repo = WeeklyTermTrendsRepository(session)
        self.regression_repo = TermRegressionResultRepository(session)
    
    def calculate_week_range(self, execution_date: date) -> tuple[date, date]:
        # 実行日が月曜日であることを確認（weekday()で0が月曜日）
        if execution_date.weekday() != 0:
            logger.warning(
                f"実行日が月曜日ではありません: {execution_date} "
                f"(weekday={execution_date.weekday()})"
            )
        
        # 先週の月曜日を計算
        # 実行日から、その週の月曜日までの日数を計算して、さらに7日引く
        days_since_monday = execution_date.weekday()
        week_start = execution_date - timedelta(days=days_since_monday + 7)
        # 先週の日曜日を計算（月曜日の6日後）
        week_end = week_start + timedelta(days=6)
        
        return week_start, week_end
    
    def validate_data_collection(
        self,
        start_date: date,
        end_date: date,
        board_key: str,
    ) -> set[date]:
        valid_dates = set()
        invalid_dates = []
        
        # 各日についてpipeline_runsを確認
        current_date = start_date
        while current_date <= end_date:
            pipeline_run = self.pipeline_run_repo.get_by_date_and_board(
                current_date,
                board_key,
            )
            
            if pipeline_run and (
                pipeline_run.status == 'success' or pipeline_run.is_recovered
            ):
                valid_dates.add(current_date)
            else:
                invalid_dates.append(current_date)
                status = pipeline_run.status if pipeline_run else 'not_found'
                is_recovered = pipeline_run.is_recovered if pipeline_run else False
                logger.warning(
                    f"データ取得の妥当性チェック失敗: "
                    f"date={current_date}, board_key={board_key}, "
                    f"status={status}, is_recovered={is_recovered}"
                )
            
            current_date += timedelta(days=1)
        
        if invalid_dates:
            logger.warning(
                f"データ取得に問題があった日数: {len(invalid_dates)}, "
                f"日付: {invalid_dates}"
            )
        
        return valid_dates
    
    def process_weekly_analysis(
        self,
        execution_date: date,
        board_key: str,
    ) -> WeeklyProcessorMetrics:
        metrics = WeeklyProcessorMetrics()
        metrics.start_time = datetime.now()
        
        # 1. 分析対象週の決定
        week_start, week_end = self.calculate_week_range(execution_date)
        logger.info(
            f"週次データ分析開始: board_key={board_key}, "
            f"week_start={week_start}, week_end={week_end}"
        )
        
        # 2. データ取得の妥当性チェック
        valid_dates = self.validate_data_collection(
            week_start,
            week_end,
            board_key,
        )
        metrics.invalid_dates = [
            d for d in [week_start + timedelta(days=i) for i in range(7)]
            if d not in valid_dates
        ]
        
        if not valid_dates:
            logger.error(
                f"有効なデータがありません: board_key={board_key}, "
                f"week_start={week_start}, week_end={week_end}"
            )
            metrics.end_time = datetime.now()
            return metrics
        
        # 3. 週次集計データの取得（有効な日のみ）
        weekly_aggregation = self.daily_stats_repo.get_weekly_aggregation(
            week_start,
            week_end,
            board_key,
            valid_dates=valid_dates,
        )
        
        # 有効な日のみのtotal_postsを取得
        total_posts = 0
        for valid_date in valid_dates:
            daily_metrics = self.metrics_repo.get_by_date_and_board(
                valid_date,
                board_key,
            )
            if daily_metrics:
                total_posts += daily_metrics.fetched_posts
        
        logger.info(
            f"週次集計完了: term_count={len(weekly_aggregation)}, "
            f"total_posts={total_posts}"
        )
        
        # 4. 各名詞について処理
        for term_data in weekly_aggregation:
            term_id = term_data['term_id']
            post_hits = term_data['post_hits']
            
            try:
                # 出現率と信頼区間の計算
                appearance_rate = (
                    post_hits / total_posts if total_posts > 0 else 0.0
                )
                ci_lower, ci_upper = calculate_appearance_rate_ci(
                    post_hits,
                    total_posts,
                )
                
                # z-scoreの計算
                zscore = self._calculate_zscore_for_term(
                    term_id,
                    board_key,
                    week_start,
                    appearance_rate,
                )
                
                # weekly_term_trendsに保存
                weekly_trend = WeeklyTermTrends(
                    week_start_date=week_start,
                    board_key=board_key,
                    term_id=term_id,
                    post_hits=post_hits,
                    total_posts=total_posts,
                    appearance_rate=appearance_rate,
                    appearance_rate_ci_lower=ci_lower,
                    appearance_rate_ci_upper=ci_upper,
                    zscore=zscore,
                )
                self.weekly_trends_repo.upsert(weekly_trend)
                
                # 回帰分析の実行
                self._perform_regression_analysis(
                    term_id,
                    board_key,
                    week_start,
                )
                
                metrics.processed_terms += 1
                
            except Exception as e:
                logger.error(
                    f"名詞処理エラー: term_id={term_id}, error={str(e)}",
                    exc_info=True,
                )
                metrics.error_terms += 1
        
        metrics.end_time = datetime.now()
        logger.info(
            f"週次データ分析完了: processed_terms={metrics.processed_terms}, "
            f"error_terms={metrics.error_terms}, "
            f"duration_sec={metrics.duration_sec}"
        )
        
        return metrics
    
    def _calculate_zscore_for_term(
        self,
        term_id: int,
        board_key: str,
        current_week_start: date,
        current_appearance_rate: float,
    ) -> Optional[float]:
        # 過去8週間のデータを取得
        eight_weeks_ago = current_week_start - timedelta(days=7 * 7)
        
        weekly_data = self.weekly_trends_repo.get_by_term_and_week_range(
            term_id,
            board_key,
            eight_weeks_ago,
            current_week_start,
            order_asc=False,
        )
        
        # 過去7週間の出現率を取得（週0を除く）
        historical_rates = [
            w.appearance_rate
            for w in weekly_data
            if w.week_start_date < current_week_start
        ]
        
        # 過去7週間のデータが不足している場合はNone
        if len(historical_rates) < 7:
            return None
        
        # z-scoreを計算
        return calculate_zscore(current_appearance_rate, historical_rates)
    
    def _perform_regression_analysis(
        self,
        term_id: int,
        board_key: str,
        current_week_start: date,
    ) -> None:
        # 過去8週間のデータを取得
        eight_weeks_ago = current_week_start - timedelta(days=7 * 7)
        
        weekly_data = self.weekly_trends_repo.get_by_term_and_week_range(
            term_id,
            board_key,
            eight_weeks_ago,
            current_week_start,
            order_asc=True,
        )
        
        # 8週間のデータが不足している場合はスキップ（既存データを保持）
        if len(weekly_data) < 8:
            return
        
        # 週番号と出現率を準備
        weeks = list(range(8))  # 0, 1, 2, ..., 7
        appearance_rates = [w.appearance_rate for w in weekly_data]
        
        # 回帰分析を実行
        regression_result = perform_linear_regression(weeks, appearance_rates)
        
        if regression_result is None:
            # エラーの場合は既存データを保持
            return
        
        # term_regression_resultsに保存
        regression = TermRegressionResult(
            board_key=board_key,
            term_id=term_id,
            intercept=regression_result['intercept'],
            slope=regression_result['slope'],
            intercept_ci_lower=regression_result['intercept_ci_lower'],
            intercept_ci_upper=regression_result['intercept_ci_upper'],
            slope_ci_lower=regression_result['slope_ci_lower'],
            slope_ci_upper=regression_result['slope_ci_upper'],
            p_value=regression_result['p_value'],
            analysis_start_date=eight_weeks_ago,
            analysis_end_date=current_week_start + timedelta(days=6),
        )
        self.regression_repo.upsert(regression)

