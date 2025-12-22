from datetime import date, datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    Double,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    board_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        nullable=True,
    )
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    code_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    
    __table_args__ = (
        CheckConstraint(
            "status IN ('success', 'failed', 'partial')",
            name="check_pipeline_runs_status",
        ),
        UniqueConstraint("target_date", "board_key", name="uq_pipeline_runs_date_board"),
    )
    
    # リレーション
    metrics: Mapped[list["PipelineMetricsDaily"]] = relationship(
        "PipelineMetricsDaily",
        back_populates="pipeline_run",
    )


class Term(Base):
    __tablename__ = "terms"
    
    term_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    normalized: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    needs_review: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    is_blocked: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    blocked_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    surface_examples: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(Text),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    
    # リレーション
    daily_stats: Mapped[list["DailyTermStats"]] = relationship(
        "DailyTermStats",
        back_populates="term",
    )
    weekly_trends: Mapped[list["WeeklyTermTrends"]] = relationship(
        "WeeklyTermTrends",
        back_populates="term",
    )
    regression_results: Mapped[list["TermRegressionResult"]] = relationship(
        "TermRegressionResult",
        back_populates="term",
    )


class DailyTermStats(Base):
    __tablename__ = "daily_term_stats"
    
    date: Mapped[date] = mapped_column(Date, nullable=False, primary_key=True)
    board_key: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("terms.term_id"),
        nullable=False,
        primary_key=True,
    )
    post_hits: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    thread_hits: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    
    __table_args__ = (
        CheckConstraint("post_hits >= 0", name="check_daily_term_stats_post_hits"),
        CheckConstraint("thread_hits >= 0", name="check_daily_term_stats_thread_hits"),
    )
    
    # リレーション
    term: Mapped["Term"] = relationship("Term", back_populates="daily_stats")


class WeeklyTermTrends(Base):
    __tablename__ = "weekly_term_trends"
    
    week_start_date: Mapped[date] = mapped_column(Date, nullable=False, primary_key=True)
    board_key: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("terms.term_id"),
        nullable=False,
        primary_key=True,
    )
    post_hits: Mapped[int] = mapped_column(Integer, nullable=False)
    total_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    appearance_rate: Mapped[float] = mapped_column(Double, nullable=False)
    appearance_rate_ci_lower: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    appearance_rate_ci_upper: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    zscore: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    
    __table_args__ = (
        CheckConstraint("post_hits >= 0", name="check_weekly_term_trends_post_hits"),
        CheckConstraint("total_posts >= 0", name="check_weekly_term_trends_total_posts"),
        CheckConstraint(
            "appearance_rate >= 0 AND appearance_rate <= 1",
            name="check_weekly_term_trends_appearance_rate",
        ),
        CheckConstraint(
            "appearance_rate_ci_lower >= 0 AND appearance_rate_ci_lower <= 1",
            name="check_weekly_term_trends_ci_lower",
        ),
        CheckConstraint(
            "appearance_rate_ci_upper >= 0 AND appearance_rate_ci_upper <= 1",
            name="check_weekly_term_trends_ci_upper",
        ),
    )
    
    # リレーション
    term: Mapped["Term"] = relationship("Term", back_populates="weekly_trends")


class TermRegressionResult(Base):
    __tablename__ = "term_regression_results"
    
    board_key: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("terms.term_id"),
        nullable=False,
        primary_key=True,
    )
    intercept: Mapped[float] = mapped_column(Double, nullable=False)
    slope: Mapped[float] = mapped_column(Double, nullable=False)
    intercept_ci_lower: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    intercept_ci_upper: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    slope_ci_lower: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    slope_ci_upper: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    p_value: Mapped[Optional[float]] = mapped_column(Double, nullable=True)
    analysis_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    analysis_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    
    __table_args__ = (
        CheckConstraint(
            "p_value >= 0 AND p_value <= 1",
            name="check_term_regression_results_p_value",
        ),
    )
    
    # リレーション
    term: Mapped["Term"] = relationship("Term", back_populates="regression_results")


class PipelineMetricsDaily(Base):
    __tablename__ = "pipeline_metrics_daily"
    
    date: Mapped[date] = mapped_column(Date, nullable=False, primary_key=True)
    board_key: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    run_id: Mapped[Optional[UUID]] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("pipeline_runs.run_id"),
        nullable=True,
    )
    fetched_threads: Mapped[int] = mapped_column(Integer, nullable=False)
    fetched_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    parsed_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    parse_fail_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    tokenize_fail_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    filtered_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    filtered_rate: Mapped[float] = mapped_column(Double, nullable=False)
    duration_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    
    __table_args__ = (
        CheckConstraint(
            "fetched_threads >= 0",
            name="check_pipeline_metrics_daily_fetched_threads",
        ),
        CheckConstraint(
            "fetched_posts >= 0",
            name="check_pipeline_metrics_daily_fetched_posts",
        ),
        CheckConstraint(
            "parsed_posts >= 0",
            name="check_pipeline_metrics_daily_parsed_posts",
        ),
        CheckConstraint(
            "parse_fail_posts >= 0",
            name="check_pipeline_metrics_daily_parse_fail_posts",
        ),
        CheckConstraint(
            "tokenize_fail_posts >= 0",
            name="check_pipeline_metrics_daily_tokenize_fail_posts",
        ),
        CheckConstraint(
            "filtered_tokens >= 0",
            name="check_pipeline_metrics_daily_filtered_tokens",
        ),
        CheckConstraint(
            "total_tokens >= 0",
            name="check_pipeline_metrics_daily_total_tokens",
        ),
        CheckConstraint(
            "filtered_rate >= 0 AND filtered_rate <= 1",
            name="check_pipeline_metrics_daily_filtered_rate",
        ),
        CheckConstraint(
            "duration_sec >= 0",
            name="check_pipeline_metrics_daily_duration_sec",
        ),
    )
    
    # リレーション
    pipeline_run: Mapped[Optional["PipelineRun"]] = relationship(
        "PipelineRun",
        back_populates="metrics",
    )

