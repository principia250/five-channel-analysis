import os
import sys
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

# srcモジュールをインポートできるようにパスを追加
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analysis.daily_processor import DailyProcessor
from src.analysis.weekly_processor import WeeklyProcessor
from src.database.models import PipelineRun
from src.database.repositories import PipelineRunRepository
from src.database.session import get_db
from src.scraping.daily_scraper import collect_posts_for_date

# JSTタイムゾーンの定義
JST = timezone(timedelta(hours=9))

# ロガーの設定
logger = logging.getLogger(__name__)

# 設定値（環境変数から取得、デフォルト値あり）
SCRAPING_BASE_URL = os.getenv("SCRAPING_BASE_URL", "https://medaka.5ch.net")
SCRAPING_BOARD_PATH = os.getenv("SCRAPING_BOARD_PATH", "/prog/")
SCRAPING_BOARD_KEY = os.getenv("SCRAPING_BOARD_KEY", "prog")


def get_target_date_jst(execution_date: Optional[datetime] = None) -> date:
    if execution_date is None:
        # 現在時刻から計算
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        now_jst = now_utc.astimezone(JST)
        return now_jst.date() - timedelta(days=1)
    
    # execution_dateはUTCなので、JSTに変換
    execution_jst = execution_date.replace(tzinfo=timezone.utc).astimezone(JST)
    # 実行日の前日が対象日
    target_date = execution_jst.date() - timedelta(days=1)
    return target_date


def run_daily_collection(**context) -> None:
    execution_date = context.get("execution_date")
    if execution_date is None:
        execution_date = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # JSTでの対象日を取得
    target_date = get_target_date_jst(execution_date)
    
    logger.info(f"日次データ収集開始: target_date={target_date}, board_key={SCRAPING_BOARD_KEY}")
    
    run_id = uuid4()
    pipeline_run = None
    
    try:
        with get_db() as session:
            # 1. PipelineRunを作成
            pipeline_run = PipelineRun(
                run_id=run_id,
                target_date=target_date,
                board_key=SCRAPING_BOARD_KEY,
                status="partial",  # 開始時はpartial
                config={
                    "base_url": SCRAPING_BASE_URL,
                    "board_path": SCRAPING_BOARD_PATH,
                    "board_key": SCRAPING_BOARD_KEY,
                },
            )
            run_repo = PipelineRunRepository(session)
            pipeline_run = run_repo.create(pipeline_run)
            session.commit()
            
            logger.info(f"PipelineRun作成: run_id={run_id}, target_date={target_date}")
        
        # 2. スクレイピング実行
        posts = collect_posts_for_date(
            base_url=SCRAPING_BASE_URL,
            board_path=SCRAPING_BOARD_PATH,
            target_date=target_date,
            timeout=30,
            max_retries=3,
            backoff_factor=1.0,
            request_delay=2.0,
            max_posts=300,
        )
        
        logger.info(f"スクレイピング完了: posts={len(posts)}")
        
        # 3. 名詞抽出・分析・DB保存
        with get_db() as session:
            processor = DailyProcessor(session)
            metrics = processor.process_posts(
                posts=posts,
                target_date=target_date,
                board_key=SCRAPING_BOARD_KEY,
                run_id=run_id,
            )
            session.commit()
            
            logger.info(
                f"日次処理完了: "
                f"fetched_threads={metrics.fetched_threads}, "
                f"fetched_posts={metrics.fetched_posts}, "
                f"parsed_posts={metrics.parsed_posts}, "
                f"total_tokens={metrics.total_tokens}, "
                f"duration_sec={metrics.duration_sec}"
            )
        
        # 4. PipelineRunのステータスを更新
        with get_db() as session:
            run_repo = PipelineRunRepository(session)
            run_repo.update_status(
                run_id=run_id,
                status="success",
                finished_at=datetime.utcnow(),
            )
            session.commit()
            
            logger.info(f"PipelineRun更新完了: run_id={run_id}, status=success")
    
    except Exception as e:
        logger.error(f"日次データ収集エラー: {e}", exc_info=True)
        
        # エラー時はPipelineRunのステータスを更新
        if pipeline_run is not None:
            try:
                with get_db() as session:
                    run_repo = PipelineRunRepository(session)
                    run_repo.update_status(
                        run_id=run_id,
                        status="failed",
                        finished_at=datetime.utcnow(),
                    )
                    session.commit()
            except Exception as update_error:
                logger.error(f"PipelineRunステータス更新エラー: {update_error}", exc_info=True)
        
        raise


def run_weekly_analysis(**context) -> None:
    execution_date = context.get("execution_date")
    if execution_date is None:
        execution_date = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # execution_dateをJSTに変換
    execution_jst = execution_date.replace(tzinfo=timezone.utc).astimezone(JST)
    execution_date_jst = execution_jst.date()
    
    logger.info(
        f"週次データ分析開始: execution_date={execution_date_jst}, "
        f"board_key={SCRAPING_BOARD_KEY}"
    )
    
    try:
        with get_db() as session:
            processor = WeeklyProcessor(session)
            metrics = processor.process_weekly_analysis(
                execution_date=execution_date_jst,
                board_key=SCRAPING_BOARD_KEY,
            )
            session.commit()
            
            logger.info(
                f"週次データ分析完了: "
                f"processed_terms={metrics.processed_terms}, "
                f"error_terms={metrics.error_terms}, "
                f"invalid_dates={len(metrics.invalid_dates)}, "
                f"duration_sec={metrics.duration_sec}"
            )
    
    except Exception as e:
        logger.error(f"週次データ分析エラー: {e}", exc_info=True)
        raise


# 日次データ収集DAG
# JST 2時 = UTC 17時（前日）
daily_dag = DAG(
    dag_id="five_ch_daily_collection",
    description="5ch日次データ収集（毎日JST 2時実行）",
    schedule="0 17 * * *",  # UTC 17時 = JST 2時（翌日）
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["scraping", "daily"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)

daily_collection_task = PythonOperator(
    task_id="collect_daily_posts",
    python_callable=run_daily_collection,
    dag=daily_dag,
)

# 週次データ分析DAG
# JST 3時（月曜日） = UTC 18時（日曜日）
weekly_dag = DAG(
    dag_id="five_ch_weekly_analysis",
    description="5ch週次データ分析（毎週月曜日JST 3時実行）",
    schedule="0 18 * * 0",  # UTC 18時（日曜日） = JST 3時（月曜日）
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["analysis", "weekly"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
)

weekly_analysis_task = PythonOperator(
    task_id="analyze_weekly_data",
    python_callable=run_weekly_analysis,
    dag=weekly_dag,
)

