from datetime import date
from typing import Optional
from uuid import UUID

from sqlalchemy import and_, desc, func
from sqlalchemy.orm import Session

from src.database.models import (
    DailyTermStats,
    PipelineMetricsDaily,
    PipelineRun,
    Term,
    TermRegressionResult,
    WeeklyTermTrends,
)


class PipelineRunRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, pipeline_run: PipelineRun) -> PipelineRun:
        self.session.add(pipeline_run)
        self.session.flush()
        return pipeline_run
    
    def get_by_id(self, run_id: UUID) -> Optional[PipelineRun]:
        return self.session.query(PipelineRun).filter(
            PipelineRun.run_id == run_id
        ).first()
    
    def get_by_date_and_board(
        self,
        target_date: date,
        board_key: str,
    ) -> Optional[PipelineRun]:
        return self.session.query(PipelineRun).filter(
            and_(
                PipelineRun.target_date == target_date,
                PipelineRun.board_key == board_key,
            )
        ).first()
    
    def update_status(
        self,
        run_id: UUID,
        status: str,
        finished_at: Optional[date] = None,
    ) -> Optional[PipelineRun]:
        pipeline_run = self.get_by_id(run_id)
        if pipeline_run:
            pipeline_run.status = status
            if finished_at:
                pipeline_run.finished_at = finished_at
            self.session.flush()
        return pipeline_run
    
    def get_by_date_range_and_board(
        self,
        start_date: date,
        end_date: date,
        board_key: str,
    ) -> list[PipelineRun]:
        return self.session.query(PipelineRun).filter(
            and_(
                PipelineRun.target_date >= start_date,
                PipelineRun.target_date <= end_date,
                PipelineRun.board_key == board_key,
            )
        ).all()


class TermRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, term: Term) -> Term:
        self.session.add(term)
        self.session.flush()
        return term
    
    def get_by_id(self, term_id: int) -> Optional[Term]:
        return self.session.query(Term).filter(
            Term.term_id == term_id
        ).first()
    
    def get_by_normalized(self, normalized: str) -> Optional[Term]:
        return self.session.query(Term).filter(
            Term.normalized == normalized
        ).first()
    
    def get_or_create(self, normalized: str) -> Term:
        term = self.get_by_normalized(normalized)
        if term is None:
            term = Term(normalized=normalized)
            self.create(term)
        return term
    
    def update_blocked(
        self,
        term_id: int,
        is_blocked: bool,
        blocked_reason: Optional[str] = None,
    ) -> Optional[Term]:
        term = self.get_by_id(term_id)
        if term:
            term.is_blocked = is_blocked
            term.blocked_reason = blocked_reason
            self.session.flush()
        return term


class DailyTermStatsRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, stats: DailyTermStats) -> DailyTermStats:
        self.session.add(stats)
        self.session.flush()
        return stats
    
    def get_by_date_and_board(
        self,
        target_date: date,
        board_key: str,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> list[DailyTermStats]:
        query = self.session.query(DailyTermStats).filter(
            and_(
                DailyTermStats.date == target_date,
                DailyTermStats.board_key == board_key,
            )
        ).order_by(desc(DailyTermStats.post_hits))
        
        if limit:
            query = query.limit(limit).offset(offset)
        
        return query.all()
    
    def upsert(self, stats: DailyTermStats) -> DailyTermStats:
        existing = self.session.query(DailyTermStats).filter(
            and_(
                DailyTermStats.date == stats.date,
                DailyTermStats.board_key == stats.board_key,
                DailyTermStats.term_id == stats.term_id,
            )
        ).first()
        
        if existing:
            existing.post_hits = stats.post_hits
            existing.thread_hits = stats.thread_hits
            self.session.flush()
            return existing
        else:
            return self.create(stats)
    
    def get_weekly_aggregation(
        self,
        start_date: date,
        end_date: date,
        board_key: str,
        valid_dates: Optional[set[date]] = None,
    ) -> list[dict]:
        from sqlalchemy import func as sql_func
        
        query = self.session.query(
            DailyTermStats.term_id,
            sql_func.sum(DailyTermStats.post_hits).label('post_hits'),
            sql_func.sum(DailyTermStats.thread_hits).label('thread_hits'),
        ).filter(
            and_(
                DailyTermStats.date >= start_date,
                DailyTermStats.date <= end_date,
                DailyTermStats.board_key == board_key,
            )
        )
        
        # valid_datesが指定されている場合、その日付のみをフィルタリング
        if valid_dates is not None:
            query = query.filter(DailyTermStats.date.in_(valid_dates))
        
        results = query.group_by(DailyTermStats.term_id).all()
        
        return [
            {
                'term_id': r.term_id,
                'post_hits': int(r.post_hits) if r.post_hits else 0,
                'thread_hits': int(r.thread_hits) if r.thread_hits else 0,
            }
            for r in results
        ]


class WeeklyTermTrendsRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, trend: WeeklyTermTrends) -> WeeklyTermTrends:
        self.session.add(trend)
        self.session.flush()
        return trend
    
    def get_by_week_and_board(
        self,
        week_start_date: date,
        board_key: str,
        limit: Optional[int] = None,
    ) -> list[WeeklyTermTrends]:
        query = self.session.query(WeeklyTermTrends).filter(
            and_(
                WeeklyTermTrends.week_start_date == week_start_date,
                WeeklyTermTrends.board_key == board_key,
            )
        ).order_by(desc(WeeklyTermTrends.zscore).nulls_last())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_by_term_and_week_range(
        self,
        term_id: int,
        board_key: str,
        start_week_date: date,
        end_week_date: date,
        order_asc: bool = True,
    ) -> list[WeeklyTermTrends]:
        query = self.session.query(WeeklyTermTrends).filter(
            and_(
                WeeklyTermTrends.term_id == term_id,
                WeeklyTermTrends.board_key == board_key,
                WeeklyTermTrends.week_start_date >= start_week_date,
                WeeklyTermTrends.week_start_date <= end_week_date,
            )
        )
        
        if order_asc:
            query = query.order_by(WeeklyTermTrends.week_start_date.asc())
        else:
            query = query.order_by(WeeklyTermTrends.week_start_date.desc())
        
        return query.all()
    
    def upsert(self, trend: WeeklyTermTrends) -> WeeklyTermTrends:
        existing = self.session.query(WeeklyTermTrends).filter(
            and_(
                WeeklyTermTrends.week_start_date == trend.week_start_date,
                WeeklyTermTrends.board_key == trend.board_key,
                WeeklyTermTrends.term_id == trend.term_id,
            )
        ).first()
        
        if existing:
            existing.post_hits = trend.post_hits
            existing.total_posts = trend.total_posts
            existing.appearance_rate = trend.appearance_rate
            existing.appearance_rate_ci_lower = trend.appearance_rate_ci_lower
            existing.appearance_rate_ci_upper = trend.appearance_rate_ci_upper
            existing.zscore = trend.zscore
            self.session.flush()
            return existing
        else:
            return self.create(trend)


class TermRegressionResultRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, result: TermRegressionResult) -> TermRegressionResult:
        self.session.add(result)
        self.session.flush()
        return result
    
    def get_by_board_and_term(
        self,
        board_key: str,
        term_id: int,
    ) -> Optional[TermRegressionResult]:
        return self.session.query(TermRegressionResult).filter(
            and_(
                TermRegressionResult.board_key == board_key,
                TermRegressionResult.term_id == term_id,
            )
        ).first()
    
    def get_by_board_sorted_by_slope(
        self,
        board_key: str,
        limit: Optional[int] = None,
    ) -> list[TermRegressionResult]:
        query = self.session.query(TermRegressionResult).filter(
            TermRegressionResult.board_key == board_key
        ).order_by(desc(TermRegressionResult.slope))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def upsert(self, result: TermRegressionResult) -> TermRegressionResult:
        existing = self.get_by_board_and_term(
            result.board_key,
            result.term_id,
        )
        
        if existing:
            existing.intercept = result.intercept
            existing.slope = result.slope
            existing.intercept_ci_lower = result.intercept_ci_lower
            existing.intercept_ci_upper = result.intercept_ci_upper
            existing.slope_ci_lower = result.slope_ci_lower
            existing.slope_ci_upper = result.slope_ci_upper
            existing.p_value = result.p_value
            existing.analysis_start_date = result.analysis_start_date
            existing.analysis_end_date = result.analysis_end_date
            self.session.flush()
            return existing
        else:
            return self.create(result)


class PipelineMetricsDailyRepository:
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, metrics: PipelineMetricsDaily) -> PipelineMetricsDaily:
        self.session.add(metrics)
        self.session.flush()
        return metrics
    
    def get_by_date_and_board(
        self,
        target_date: date,
        board_key: str,
    ) -> Optional[PipelineMetricsDaily]:
        return self.session.query(PipelineMetricsDaily).filter(
            and_(
                PipelineMetricsDaily.date == target_date,
                PipelineMetricsDaily.board_key == board_key,
            )
        ).first()
    
    def upsert(self, metrics: PipelineMetricsDaily) -> PipelineMetricsDaily:
        existing = self.get_by_date_and_board(
            metrics.date,
            metrics.board_key,
        )
        
        if existing:
            existing.run_id = metrics.run_id
            existing.fetched_threads = metrics.fetched_threads
            existing.fetched_posts = metrics.fetched_posts
            existing.parsed_posts = metrics.parsed_posts
            existing.parse_fail_posts = metrics.parse_fail_posts
            existing.tokenize_fail_posts = metrics.tokenize_fail_posts
            existing.filtered_tokens = metrics.filtered_tokens
            existing.total_tokens = metrics.total_tokens
            existing.filtered_rate = metrics.filtered_rate
            existing.duration_sec = metrics.duration_sec
            self.session.flush()
            return existing
        else:
            return self.create(metrics)
    
    def get_weekly_total_posts(
        self,
        start_date: date,
        end_date: date,
        board_key: str,
    ) -> int:
        from sqlalchemy import func as sql_func
        
        result = self.session.query(
            sql_func.sum(PipelineMetricsDaily.fetched_posts)
        ).filter(
            and_(
                PipelineMetricsDaily.date >= start_date,
                PipelineMetricsDaily.date <= end_date,
                PipelineMetricsDaily.board_key == board_key,
            )
        ).scalar()
        
        return int(result) if result else 0

