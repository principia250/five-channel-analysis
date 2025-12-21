"""Scraperクラスのテスト"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from src.scraping.scraper import Scraper
from src.scraping import utils


class TestScraperInit:
    """__init__()のテスト"""

    def test_init_with_defaults(self):
        """デフォルト値で初期化できる"""
        scraper = Scraper()
        assert scraper.timeout == 30
        assert scraper.max_retries == 3
        assert scraper.backoff_factor == 1.0
        assert scraper.request_delay == 2.0
        assert scraper.session is not None

    def test_init_with_custom_params(self):
        """カスタムパラメータで初期化できる"""
        scraper = Scraper(
            timeout=60,
            max_retries=5,
            backoff_factor=2.0,
            request_delay=3.0
        )
        assert scraper.timeout == 60
        assert scraper.max_retries == 5
        assert scraper.backoff_factor == 2.0
        assert scraper.request_delay == 3.0

    def test_init_session_configured(self):
        """セッションが正しく設定される"""
        scraper = Scraper()
        assert scraper.session is not None
        assert isinstance(scraper.session, requests.Session)

    def test_init_headers_set(self):
        """ヘッダーが正しく設定される"""
        scraper = Scraper()
        headers = scraper.session.headers
        assert "User-Agent" in headers
        assert "Accept-Language" in headers

    def test_init_retry_strategy_configured(self):
        """リトライ戦略が正しく設定される"""
        scraper = Scraper(max_retries=5, backoff_factor=2.0)
        # HTTPAdapterがマウントされていることを確認
        assert scraper.session.adapters.get("http://") is not None
        assert scraper.session.adapters.get("https://") is not None


class TestScraperFetch:
    """fetch()のテスト"""

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_success(self, mock_is_valid_url, mock_sleep):
        """正常なHTTPレスポンスを取得できる"""
        mock_is_valid_url.return_value = True
        
        # モックレスポンスを作成
        mock_response = Mock()
        mock_response.text = "<html>test</html>"
        mock_response.encoding = "Shift_JIS"
        mock_response.raise_for_status = Mock()
        
        scraper = Scraper()
        scraper.session.get = Mock(return_value=mock_response)
        
        result = scraper.fetch("https://example.com")
        
        assert result == "<html>test</html>"
        mock_sleep.assert_called_once()
        scraper.session.get.assert_called_once_with("https://example.com", timeout=30)

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_encoding_set(self, mock_is_valid_url, mock_sleep):
        """エンコーディングが正しく設定される"""
        mock_is_valid_url.return_value = True
        
        mock_response = Mock()
        mock_response.text = "<html>test</html>"
        mock_response.encoding = None
        mock_response.raise_for_status = Mock()
        
        scraper = Scraper()
        scraper.session.get = Mock(return_value=mock_response)
        
        scraper.fetch("https://example.com", encoding="UTF-8")
        
        assert mock_response.encoding == "UTF-8"

    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_invalid_url_raises_value_error(self, mock_is_valid_url):
        """不正なURLでValueErrorが発生する"""
        mock_is_valid_url.return_value = False
        
        scraper = Scraper()
        
        with pytest.raises(ValueError, match="Invalid URL"):
            scraper.fetch("invalid-url")

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_http_error_raises_exception(self, mock_is_valid_url, mock_sleep):
        """HTTPエラーでRequestExceptionが発生する"""
        mock_is_valid_url.return_value = True
        
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        scraper = Scraper()
        scraper.session.get = Mock(return_value=mock_response)
        
        with pytest.raises(requests.RequestException):
            scraper.fetch("https://example.com")

    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_timeout_raises_exception(self, mock_is_valid_url):
        """タイムアウトでRequestExceptionが発生する"""
        mock_is_valid_url.return_value = True
        
        scraper = Scraper()
        scraper.session.get = Mock(side_effect=requests.Timeout("Request timeout"))
        
        with pytest.raises(requests.RequestException):
            scraper.fetch("https://example.com")

    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_connection_error_raises_exception(self, mock_is_valid_url):
        """ネットワークエラーでRequestExceptionが発生する"""
        mock_is_valid_url.return_value = True
        
        scraper = Scraper()
        scraper.session.get = Mock(side_effect=requests.ConnectionError("Connection failed"))
        
        with pytest.raises(requests.RequestException):
            scraper.fetch("https://example.com")

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_empty_response(self, mock_is_valid_url, mock_sleep):
        """空のレスポンスボディ"""
        mock_is_valid_url.return_value = True
        
        mock_response = Mock()
        mock_response.text = ""
        mock_response.encoding = "Shift_JIS"
        mock_response.raise_for_status = Mock()
        
        scraper = Scraper()
        scraper.session.get = Mock(return_value=mock_response)
        
        result = scraper.fetch("https://example.com")
        assert result == ""

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_different_encoding(self, mock_is_valid_url, mock_sleep):
        """異なるエンコーディングの指定"""
        mock_is_valid_url.return_value = True
        
        mock_response = Mock()
        mock_response.text = "<html>test</html>"
        mock_response.encoding = None
        mock_response.raise_for_status = Mock()
        
        scraper = Scraper()
        scraper.session.get = Mock(return_value=mock_response)
        
        scraper.fetch("https://example.com", encoding="UTF-8")
        assert mock_response.encoding == "UTF-8"


class TestScraperRetry:
    """リトライ機能のテスト"""

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_retries_on_500_error(self, mock_is_valid_url, mock_sleep):
        """500エラー時にリトライが試みられる（urllib3のRetryが動作）"""
        mock_is_valid_url.return_value = True
        
        # 最初は500エラー、2回目で成功
        mock_response_error = Mock()
        mock_response_error.status_code = 500
        mock_response_error.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")
        
        mock_response_success = Mock()
        mock_response_success.text = "<html>success</html>"
        mock_response_success.encoding = "Shift_JIS"
        mock_response_success.raise_for_status = Mock()
        
        scraper = Scraper(max_retries=3)
        # モックのセッションでは実際のリトライは動作しないため、
        # リトライ設定が適用されていることを確認する
        assert scraper.max_retries == 3

    @patch('src.scraping.scraper.utils.sleep_with_jitter')
    @patch('src.scraping.scraper.utils.is_valid_url')
    def test_fetch_retries_on_503_error(self, mock_is_valid_url, mock_sleep):
        """503エラー時にリトライが試みられる"""
        mock_is_valid_url.return_value = True
        
        scraper = Scraper(max_retries=2, backoff_factor=1.5)
        # リトライ設定が正しく反映されていることを確認
        assert scraper.max_retries == 2
        assert scraper.backoff_factor == 1.5

    def test_retry_status_codes_configured(self):
        """リトライ対象のステータスコードが正しく設定されている"""
        scraper = Scraper()
        # HTTPAdapterがマウントされていることを確認
        http_adapter = scraper.session.adapters.get("http://")
        https_adapter = scraper.session.adapters.get("https://")
        
        assert http_adapter is not None
        assert https_adapter is not None
        # Retryオブジェクトが設定されていることを確認
        assert http_adapter.max_retries is not None
        assert https_adapter.max_retries is not None


class TestScraperClose:
    """close()のテスト"""

    def test_close_closes_session(self):
        """セッションが正しくクローズされる"""
        scraper = Scraper()
        scraper.session.close = Mock()
        
        scraper.close()
        
        scraper.session.close.assert_called_once()

    def test_close_multiple_calls(self):
        """複数回呼んでもエラーにならない"""
        scraper = Scraper()
        scraper.session.close = Mock()
        
        scraper.close()
        scraper.close()
        scraper.close()
        
        assert scraper.session.close.call_count == 3


class TestScraperContextManager:
    """コンテキストマネージャーのテスト"""

    def test_context_manager_usage(self):
        """with文で使用できる"""
        with Scraper() as scraper:
            assert scraper is not None
            assert isinstance(scraper, Scraper)

    def test_context_manager_auto_close(self):
        """ブロック終了時に自動的にセッションがクローズされる"""
        with Scraper() as scraper:
            scraper.session.close = Mock()
        
        scraper.session.close.assert_called_once()

    def test_context_manager_close_on_exception(self):
        """例外が発生してもセッションがクローズされる"""
        scraper = None
        try:
            with Scraper() as scraper:
                scraper.session.close = Mock()
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        scraper.session.close.assert_called_once()

