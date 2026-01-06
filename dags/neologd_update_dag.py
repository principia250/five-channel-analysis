import os
import sys
import logging
from datetime import datetime, timedelta, timezone

# srcモジュールをインポートできるようにパスを追加
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.neologd_updater import NeologdUpdater

# JSTタイムゾーンの定義
JST = timezone(timedelta(hours=9))

# ロガーの設定
logger = logging.getLogger(__name__)


def update_neologd(**context) -> None:
    logger.info("NEologd辞書の更新チェックを開始")
    
    try:
        updater = NeologdUpdater()
        
        # 現在のバージョンを確認
        current_version = updater.get_current_version()
        latest_version = updater.get_latest_version()
        
        logger.info(f"現在のバージョン: {current_version}")
        logger.info(f"最新バージョン: {latest_version}")
        
        if latest_version is None:
            logger.error("最新バージョンの取得に失敗しました")
            raise Exception("最新バージョンの取得に失敗")
        
        # 更新が必要か確認
        if current_version == latest_version:
            logger.info("既に最新版がインストールされています")
            return
        
        # 更新を実行
        logger.info(f"辞書の更新を開始: {current_version} -> {latest_version}")
        success = updater.update(force=False)
        
        if not success:
            raise Exception("辞書の更新に失敗しました")
        
        # 動作確認
        logger.info("更新後の動作確認を実行")
        if not updater.verify_installation():
            logger.warning("動作確認に失敗しましたが、更新は完了しています")
        
        logger.info(f"NEologd辞書の更新が完了: {latest_version}")
        
    except Exception as e:
        logger.error(f"NEologd辞書の更新エラー: {e}", exc_info=True)
        raise


# NEologd更新DAG
# 毎月1日JST 3時 = UTC 18時（前日）
neologd_update_dag = DAG(
    dag_id="neologd_update",
    description="NEologd辞書の自動更新（毎月1日JST 3時実行）",
    schedule="0 18 1 * *",  # UTC 18時（前日） = JST 3時（1日）
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["maintenance", "neologd"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": True,  # 失敗時は通知
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(hours=1),  # 1時間後にリトライ
    },
)

update_neologd_task = PythonOperator(
    task_id="update_neologd_dictionary",
    python_callable=update_neologd,
    dag=neologd_update_dag,
)

