"""パーサーモジュールのテスト"""
import pytest
from pathlib import Path
from src.scraping.parser import parse_board_page, parse_thread_page, ThreadInfo, PostInfo


# fixturesディレクトリのパス
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "sample_html"


# pytestフィクスチャで重複コードを削減
@pytest.fixture
def board_page_html():
    """板ページのHTMLフィクスチャ"""
    html_file = FIXTURES_DIR / "board_page.html"
    return html_file.read_text(encoding="utf-8")


@pytest.fixture
def thread_page_html():
    """スレッドページのHTMLフィクスチャ"""
    html_file = FIXTURES_DIR / "thread_page.html"
    return html_file.read_text(encoding="utf-8")


class TestParseBoardPage:
    """parse_board_page()のテスト"""

    def test_parse_board_page_extracts_threads(self, board_page_html):
        """板のトップページHTMLからスレッド一覧を抽出できる"""
        result = parse_board_page(board_page_html)
        
        assert len(result) == 3
        assert all(isinstance(thread, ThreadInfo) for thread in result)
        # 複数のスレッドが抽出できることも同時に確認
        assert len(result) >= 2

    def test_parse_board_page_removes_l_suffix(self):
        """/l50などのサフィックスが正しく削除される"""
        html = '''
        <div style="background: #BEB;">
        <p style="background: #BEB;"><a href="/test/read.cgi/prog/1000000001/l50">Test</a></p>
        </div>
        '''
        
        result = parse_board_page(html)
        
        assert len(result) == 1
        assert result[0].path == "/test/read.cgi/prog/1000000001"

    def test_parse_board_page_creates_thread_info(self, board_page_html):
        """ThreadInfoオブジェクトが正しく作成される"""
        result = parse_board_page(board_page_html)
        
        assert result[0].path == "/test/read.cgi/prog/1000000001"
        assert result[1].path == "/test/read.cgi/prog/1000000002"

    def test_parse_board_page_empty_html(self):
        """空のHTML"""
        html = ""
        result = parse_board_page(html)
        assert result == []

    def test_parse_board_page_no_threads(self):
        """スレッドが0件のHTML"""
        html = "<html><body><p>No threads</p></body></html>"
        result = parse_board_page(html)
        assert result == []

    def test_parse_board_page_malformed_html(self):
        """不正な形式のHTML"""
        html = "<html><body><p>Invalid</p>"
        result = parse_board_page(html)
        # BeautifulSoupは不正なHTMLでもパースを試みる
        assert isinstance(result, list)

    def test_parse_board_page_no_thread_list(self):
        """スレッド一覧が含まれていないHTML"""
        html = "<html><body><p>Regular paragraph</p></body></html>"
        result = parse_board_page(html)
        assert result == []

    def test_parse_board_page_path_format(self, board_page_html):
        """抽出されたThreadInfoのpathが正しい形式である"""
        result = parse_board_page(board_page_html)
        
        for thread in result:
            assert thread.path.startswith("/test/read.cgi/")
            assert "/l" not in thread.path  # /l50などのサフィックスが削除されている


class TestParseThreadPage:
    """parse_thread_page()のテスト"""

    def test_parse_thread_page_extracts_posts(self, thread_page_html):
        """スレッドページHTMLから投稿一覧を抽出できる"""
        result = parse_thread_page(thread_page_html)
        
        assert len(result) == 3
        assert all(isinstance(post, PostInfo) for post in result)
        # 複数の投稿が抽出できることも同時に確認
        assert len(result) >= 2

    def test_parse_thread_page_extracts_date(self, thread_page_html):
        """日付が正しく抽出される"""
        result = parse_thread_page(thread_page_html)
        
        assert len(result) == 3
        assert result[0].date == "2025/01/01(月) 12:00:00.00"
        assert result[1].date == "2025/01/01(月) 12:01:00.00"
        assert result[2].date == "2025/01/01(月) 12:02:00.00"

    def test_parse_thread_page_extracts_content(self, thread_page_html):
        """投稿内容が正しく抽出される（HTMLタグが除去される）"""
        result = parse_thread_page(thread_page_html)
        
        assert len(result) == 3
        assert result[0].content == "これはテスト用の投稿内容です"
        assert result[1].content == "2番目のテスト投稿です"
        assert result[2].content == "3番目のテスト投稿です。改行を含む\n複数行のテキストです。"
        # HTMLタグが含まれていないことを確認
        assert "<div>" not in result[0].content
        assert "</div>" not in result[0].content
        assert "<br>" not in result[2].content
        assert "<br />" not in result[2].content

    def test_parse_thread_page_creates_post_info(self, thread_page_html):
        """PostInfoオブジェクトが正しく作成される"""
        result = parse_thread_page(thread_page_html)
        
        assert isinstance(result[0], PostInfo)
        assert hasattr(result[0], "date")
        assert hasattr(result[0], "content")

    def test_parse_thread_page_empty_html(self):
        """空のHTML"""
        html = ""
        result = parse_thread_page(html)
        assert result == []

    def test_parse_thread_page_no_posts(self):
        """投稿が0件のHTML"""
        html = "<html><body><div>No posts</div></body></html>"
        result = parse_thread_page(html)
        assert result == []

    def test_parse_thread_page_missing_date(self):
        """日付が欠落している投稿（スキップされる）"""
        html = '''
        <div class="clear post">
        <div class="post-header"></div>
        <div class="post-content">Content without date</div>
        </div>
        '''
        result = parse_thread_page(html)
        assert result == []

    def test_parse_thread_page_empty_content(self):
        """内容が空の投稿（スキップされる）"""
        html = '''
        <div class="clear post">
        <div class="post-header">
        <span class="date">2025/12/02(火) 10:50:43.07</span>
        </div>
        <div class="post-content"></div>
        </div>
        '''
        result = parse_thread_page(html)
        assert result == []

    def test_parse_thread_page_malformed_html(self):
        """不正な形式のHTML"""
        html = "<html><body><div>Invalid</div>"
        result = parse_thread_page(html)
        # BeautifulSoupは不正なHTMLでもパースを試みる
        assert isinstance(result, list)

    def test_parse_thread_page_date_format(self, thread_page_html):
        """抽出されたPostInfoのdateが正しい形式である"""
        result = parse_thread_page(thread_page_html)
        
        for post in result:
            assert post.date is not None
            assert len(post.date) > 0

    def test_parse_thread_page_no_html_tags_in_content(self):
        """抽出されたPostInfoのcontentにHTMLタグが含まれていない"""
        html = '''
        <div class="clear post">
        <div class="post-header">
        <span class="date">2025/12/02(火) 10:50:43.07</span>
        </div>
        <div class="post-content">
        <p>Test content with <b>bold</b> text</p>
        </div>
        </div>
        '''
        result = parse_thread_page(html)
        
        assert len(result) == 1
        assert "<p>" not in result[0].content
        assert "<b>" not in result[0].content
        assert "</b>" not in result[0].content
        assert "</p>" not in result[0].content
        # テキスト内容が正しく抽出されていることを確認（改行を考慮）
        # get_text(separator='\n')により改行が入るため、改行を除去して比較
        content_without_newlines = result[0].content.replace('\n', ' ')
        assert "Test content with bold text" in content_without_newlines

    def test_parse_thread_page_newline_handling(self):
        """改行が正しく処理されている（`\\n`でseparator指定）"""
        html = '''
        <div class="clear post">
        <div class="post-header">
        <span class="date">2025/12/02(火) 10:50:43.07</span>
        </div>
        <div class="post-content">
        Line 1<br>Line 2<br>Line 3
        </div>
        </div>
        '''
        result = parse_thread_page(html)
        
        assert len(result) == 1
        # get_text(separator='\n')により改行が保持されることを明確に確認
        assert "\n" in result[0].content
        assert "Line 1" in result[0].content
        assert "Line 2" in result[0].content
        assert "Line 3" in result[0].content
        # 改行で区切られていることを確認
        lines = result[0].content.split("\n")
        assert len(lines) >= 3

