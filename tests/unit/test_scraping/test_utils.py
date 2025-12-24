"""utilsモジュールのテスト"""
import pytest
from unittest.mock import patch, MagicMock
from src.scraping import utils


class TestBuildUrl:
    """build_url()のテスト"""

    def test_build_url_basic(self):
        """基本的なURL結合が動作することを確認"""
        result = utils.build_url("https://example.com", "/path")
        assert result == "https://example.com/path"

    def test_build_url_with_relative_path(self):
        """相対パスの結合"""
        result = utils.build_url("https://example.com/base/", "relative")
        assert result == "https://example.com/base/relative"

    def test_build_url_with_absolute_path(self):
        """絶対パスの結合（ベースURLが上書きされる）"""
        result = utils.build_url("https://example.com/base/", "/absolute")
        assert result == "https://example.com/absolute"

    def test_build_url_with_query_string(self):
        """クエリパラメータ付きパス"""
        result = utils.build_url("https://example.com", "/path?key=value")
        assert result == "https://example.com/path?key=value"

    def test_build_url_with_fragment(self):
        """フラグメント付きパス"""
        result = utils.build_url("https://example.com", "/path#section")
        assert result == "https://example.com/path#section"

    def test_build_url_with_existing_path(self):
        """既存のパスがあるベースURL"""
        result = utils.build_url("https://example.com/base", "path")
        assert result == "https://example.com/path"

    def test_build_url_with_trailing_slash(self):
        """末尾スラッシュがあるベースURL"""
        result = utils.build_url("https://example.com/", "path")
        assert result == "https://example.com/path"

    def test_build_url_empty_path(self):
        """空のパス"""
        result = utils.build_url("https://example.com", "")
        assert result == "https://example.com/"

    def test_build_url_path_with_multiple_segments(self):
        """複数のセグメントを含むパス"""
        result = utils.build_url("https://example.com", "/path/to/resource")
        assert result == "https://example.com/path/to/resource"


class TestIsValidUrl:
    """is_valid_url()のテスト"""

    def test_valid_http_url(self):
        """有効なHTTP URL"""
        assert utils.is_valid_url("http://example.com") is True

    def test_valid_https_url(self):
        """有効なHTTPS URL"""
        assert utils.is_valid_url("https://example.com") is True

    def test_valid_url_with_path(self):
        """パス付きURL"""
        assert utils.is_valid_url("https://example.com/path/to/page") is True

    def test_valid_url_with_query(self):
        """クエリパラメータ付きURL"""
        assert utils.is_valid_url("https://example.com?key=value") is True

    def test_invalid_url_no_scheme(self):
        """スキームなし"""
        assert utils.is_valid_url("example.com") is False

    def test_invalid_url_no_host(self):
        """ホスト名なし"""
        assert utils.is_valid_url("http://") is False

    def test_invalid_url_empty_string(self):
        """空文字列"""
        assert utils.is_valid_url("") is False

    def test_invalid_url_malformed(self):
        """不正な形式のURL"""
        assert utils.is_valid_url("not a url") is False


class TestGetDefaultHeaders:
    """get_default_headers()のテスト（最小限）"""

    def test_headers_contain_user_agent(self):
        """User-Agentが含まれている"""
        headers = utils.get_default_headers()
        assert "User-Agent" in headers
        assert "Mozilla" in headers["User-Agent"]


class TestSleepWithJitter:
    """sleep_with_jitter()のテスト"""

    @patch('src.scraping.utils.time.sleep')
    @patch('random.uniform')
    def test_sleep_with_jitter_calls_sleep(self, mock_uniform, mock_sleep):
        """sleepが呼ばれることを確認"""
        mock_uniform.return_value = 0.2
        utils.sleep_with_jitter(1.0, 0.5)
        mock_sleep.assert_called_once()
        # max(0, 1.0 + 0.2) = 1.2
        mock_sleep.assert_called_with(1.2)

    @patch('src.scraping.utils.time.sleep')
    @patch('random.uniform')
    def test_sleep_with_jitter_negative_result(self, mock_uniform, mock_sleep):
        """負の値にならないことを確認"""
        mock_uniform.return_value = -2.0  # base_seconds + jitter が負になる
        utils.sleep_with_jitter(1.0, 0.5)
        # max(0, 1.0 + (-2.0)) = max(0, -1.0) = 0
        mock_sleep.assert_called_with(0)

    @patch('src.scraping.utils.time.sleep')
    @patch('random.uniform')
    def test_sleep_with_jitter_zero_base(self, mock_uniform, mock_sleep):
        """base_secondsが0の場合"""
        mock_uniform.return_value = 0.1
        utils.sleep_with_jitter(0.0, 0.5)
        mock_sleep.assert_called_once()
        # max(0, 0.0 + 0.1) = 0.1
        mock_sleep.assert_called_with(0.1)

    @patch('src.scraping.utils.time.sleep')
    @patch('random.uniform')
    def test_sleep_with_jitter_large_jitter(self, mock_uniform, mock_sleep):
        """jitter_rangeがbase_secondsより大きい場合"""
        mock_uniform.return_value = 1.5
        utils.sleep_with_jitter(1.0, 2.0)
        mock_sleep.assert_called_once()
        # max(0, 1.0 + 1.5) = 2.5
        mock_sleep.assert_called_with(2.5)


class TestExtractThreadIdFromUrl:
    """extract_thread_id_from_url()のテスト"""

    def test_extract_thread_id_with_trailing_slash(self):
        """末尾スラッシュありのURLからスレッドIDを抽出"""
        url = "/test/read.cgi/prog/1764640243/"
        result = utils.extract_thread_id_from_url(url)
        assert result == "1764640243"

    def test_extract_thread_id_without_trailing_slash(self):
        """末尾スラッシュなしのURLからスレッドIDを抽出"""
        url = "/test/read.cgi/prog/1764640243"
        result = utils.extract_thread_id_from_url(url)
        assert result == "1764640243"

    def test_extract_thread_id_from_full_url(self):
        """完全URLからスレッドIDを抽出"""
        url = "https://medaka.5ch.net/test/read.cgi/prog/1764640243/"
        result = utils.extract_thread_id_from_url(url)
        assert result == "1764640243"

    def test_extract_thread_id_with_l_suffix(self):
        """/l50などのサフィックスが含まれる場合"""
        url = "/test/read.cgi/prog/1764640243/l50"
        result = utils.extract_thread_id_from_url(url)
        assert result == "1764640243"

    def test_extract_thread_id_no_read_cgi(self):
        """read.cgiが含まれていないURL"""
        url = "https://example.com/path/to/page"
        result = utils.extract_thread_id_from_url(url)
        assert result is None

    def test_extract_thread_id_invalid_format(self):
        """不正な形式のURL"""
        url = "not a valid url"
        result = utils.extract_thread_id_from_url(url)
        assert result is None

    def test_extract_thread_id_empty_string(self):
        """空文字列"""
        url = ""
        result = utils.extract_thread_id_from_url(url)
        assert result is None

    def test_extract_thread_id_insufficient_path_parts(self):
        """パスパーツが不足している場合"""
        url = "/test/read.cgi/prog"
        result = utils.extract_thread_id_from_url(url)
        assert result is None

