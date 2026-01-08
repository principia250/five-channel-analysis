from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# NEologdのGitHubリポジトリ情報
NEOLOGD_REPO = "neologd/mecab-ipadic-neologd"
NEOLOGD_GITHUB_API = f"https://api.github.com/repos/{NEOLOGD_REPO}/releases/latest"
NEOLOGD_INSTALL_DIR = Path("/opt/airflow/neologd")
NEOLOGD_VERSION_FILE = NEOLOGD_INSTALL_DIR / ".version"
NEOLOGD_DICTIONARY_PATH = Path("/opt/airflow/mecab-ipadic-neologd")


class NeologdUpdater:
    def __init__(self, install_dir: Optional[Path] = None):
        self.install_dir = Path(install_dir) if install_dir else NEOLOGD_INSTALL_DIR
        self.version_file = self.install_dir / ".version"
        self.dict_dir = self.install_dir / "mecab-ipadic-neologd"

    def get_latest_version(self) -> Optional[str]:
        try:
            response = requests.get(
                NEOLOGD_GITHUB_API,
                timeout=10,
                headers={"Accept": "application/vnd.github.v3+json"},
            )
            response.raise_for_status()
            data = response.json()
            version = data.get("tag_name", "").lstrip("v")
            logger.info(f"最新バージョンを取得: {version}")
            return version
        except requests.RequestException as e:
            logger.error(f"GitHub APIからのバージョン取得に失敗: {e}")
            return None
        except Exception as e:
            logger.error(f"バージョン取得時の予期しないエラー: {e}")
            return None

    def get_current_version(self) -> Optional[str]:
        if not self.version_file.exists():
            return None

        try:
            with open(self.version_file, "r", encoding="utf-8") as f:
                version = f.read().strip()
            return version if version else None
        except Exception as e:
            logger.warning(f"バージョンファイルの読み込みに失敗: {e}")
            return None

    def is_update_available(self) -> bool:
        current = self.get_current_version()
        latest = self.get_latest_version()

        if latest is None:
            return False

        if current is None:
            # 未インストールの場合はインストールが必要
            return True

        # バージョンが異なる場合は更新が必要
        return current != latest

    def clone_repository(self) -> bool:
        repo_url = f"https://github.com/{NEOLOGD_REPO}.git"
        target_dir = self.dict_dir

        # 既存のディレクトリがある場合は削除
        if target_dir.exists():
            logger.info(f"既存のディレクトリを削除: {target_dir}")
            shutil.rmtree(target_dir)

        try:
            logger.info(f"リポジトリをクローン: {repo_url}")
            result = subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(target_dir)],
                check=True,
                capture_output=True,
                text=True,
                timeout=300,  # 5分タイムアウト
            )
            logger.info("リポジトリのクローンが完了")
            return True
        except subprocess.TimeoutExpired:
            logger.error("リポジトリのクローンがタイムアウト")
            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"リポジトリのクローンに失敗: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"リポジトリクローン時の予期しないエラー: {e}")
            return False

    def build_dictionary(self) -> bool:
        if not self.dict_dir.exists():
            logger.error(f"辞書ディレクトリが存在しません: {self.dict_dir}")
            return False

        build_script = self.dict_dir / "bin" / "install-mecab-ipadic-neologd"
        if not build_script.exists():
            logger.error(f"ビルドスクリプトが存在しません: {build_script}")
            return False

        try:
            # patchコマンドが利用可能か確認
            logger.info(f"辞書のビルドを開始（時間がかかります）: {NEOLOGD_DICTIONARY_PATH}")
            result = subprocess.run(
                ["bash", str(build_script), "-n", "-y", "-u", "-p", str(NEOLOGD_DICTIONARY_PATH)],
                cwd=self.dict_dir,
                capture_output=True,
                text=True,
                timeout=1800,  # 30分タイムアウト
            )
            if result.returncode != 0:
                logger.error("辞書のビルドに失敗")
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return False
            logger.info("辞書のビルドが完了")
            return True
        except subprocess.TimeoutExpired:
            logger.error("辞書のビルドがタイムアウト")
            return False
        except subprocess.CalledProcessError as e:
            # 現状ここには来ない想定だが、安全のため残しておく
            logger.error(f"辞書のビルドに失敗: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"辞書ビルド時の予期しないエラー: {e}")
            return False

    def save_version(self, version: str) -> bool:
        try:
            self.install_dir.mkdir(parents=True, exist_ok=True)
            with open(self.version_file, "w", encoding="utf-8") as f:
                f.write(version)
            logger.info(f"バージョンを保存: {version}")
            return True
        except Exception as e:
            logger.error(f"バージョン保存に失敗: {e}")
            return False

    def get_dictionary_path(self) -> Optional[Path]:
        # 明示的に指定したインストール先を確認
        if NEOLOGD_DICTIONARY_PATH.exists() and (NEOLOGD_DICTIONARY_PATH / "dicrc").exists():
            return NEOLOGD_DICTIONARY_PATH
        return None

    def update(self, force: bool = False) -> bool:
        if not force and not self.is_update_available():
            logger.info("更新は不要です（最新版がインストール済み）")
            return True

        latest_version = self.get_latest_version()
        if latest_version is None:
            logger.error("最新バージョンの取得に失敗")
            return False

        logger.info(f"NEologd辞書の更新を開始: {latest_version}")

        # 1. リポジトリをクローン
        if not self.clone_repository():
            return False

        # 2. 辞書をビルド
        if not self.build_dictionary():
            return False

        # 3. バージョンを保存
        if not self.save_version(latest_version):
            logger.warning("バージョンの保存に失敗しましたが、辞書のビルドは完了しています")

        logger.info(f"NEologd辞書の更新が完了: {latest_version}")
        return True

    def verify_installation(self) -> bool:
        dict_path = self.get_dictionary_path()
        if dict_path is None:
            logger.warning("辞書のパスが見つかりません")
            return False

        try:
            # MeCabで辞書が使用できるかテスト
            test_text = "Pythonはプログラミング言語です"
            result = subprocess.run(
                ["mecab", "-d", str(dict_path), "-Owakati"],
                input=test_text,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                logger.info("辞書の動作確認が完了")
                return True
            else:
                logger.error(f"辞書の動作確認に失敗: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"辞書の動作確認時のエラー: {e}")
            return False

