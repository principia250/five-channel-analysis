import pytest
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock
from uuid import uuid4

from src.analysis.daily_processor import DailyProcessor, DailyProcessorMetrics
from src.database.models import Term
from src.scraping.daily_scraper import CollectedPost


@pytest.fixture
def mock_session():
    """モックセッション"""
    session = Mock()
    session.add = Mock()
    session.flush = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.query = Mock()
    return session


@pytest.fixture
def mock_term_repo():
    """モックTermRepository"""
    repo = Mock()
    return repo


@pytest.fixture
def mock_daily_stats_repo():
    """モックDailyTermStatsRepository"""
    repo = Mock()
    return repo


@pytest.fixture
def mock_metrics_repo():
    """モックPipelineMetricsDailyRepository"""
    repo = Mock()
    return repo


@pytest.fixture
def mock_noun_extractor():
    """モックNounExtractor"""
    extractor = Mock()
    return extractor


@pytest.fixture
def processor(mock_session, mock_term_repo, mock_daily_stats_repo, mock_metrics_repo, mock_noun_extractor):
    """DailyProcessorのインスタンス（モック注入）"""
    with patch('src.analysis.daily_processor.TermRepository') as mock_term_repo_class, \
         patch('src.analysis.daily_processor.DailyTermStatsRepository') as mock_daily_stats_repo_class, \
         patch('src.analysis.daily_processor.PipelineMetricsDailyRepository') as mock_metrics_repo_class, \
         patch('src.analysis.daily_processor.NounExtractor', return_value=mock_noun_extractor):
        mock_term_repo_class.return_value = mock_term_repo
        mock_daily_stats_repo_class.return_value = mock_daily_stats_repo
        mock_metrics_repo_class.return_value = mock_metrics_repo
        return DailyProcessor(mock_session)


class TestDailyProcessorMetrics:
    def test_duration_sec_calculated(self):
        """処理時間が正しく計算される"""
        metrics = DailyProcessorMetrics()
        metrics.start_time = datetime(2025, 1, 1, 12, 0, 0)
        metrics.end_time = datetime(2025, 1, 1, 12, 0, 5)
        
        assert metrics.duration_sec == 5
    
    def test_duration_sec_no_times(self):
        """開始時刻・終了時刻が設定されていない場合は0を返す"""
        metrics = DailyProcessorMetrics()
        assert metrics.duration_sec == 0
    
    def test_filtered_rate_calculated(self):
        """フィルタ率が正しく計算される"""
        metrics = DailyProcessorMetrics()
        metrics.total_tokens = 100
        metrics.filtered_tokens = 20
        
        assert metrics.filtered_rate == 0.2
    
    def test_filtered_rate_zero_tokens(self):
        """トークン数が0の場合は0.0を返す"""
        metrics = DailyProcessorMetrics()
        metrics.total_tokens = 0
        metrics.filtered_tokens = 0
        
        assert metrics.filtered_rate == 0.0


class TestDailyProcessorProcessPosts:
    """DailyProcessor.process_posts()のテスト"""
    
    def test_process_posts_basic(self, processor, mock_noun_extractor, mock_term_repo, 
                                 mock_daily_stats_repo, mock_metrics_repo):
        """基本的な処理が動作する"""
        # テストデータ
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Pythonでプログラミングを学習する",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        # モックの設定
        mock_noun_extractor.extract_nouns.return_value = ["Python", "プログラミング", "学習"]
        
        term1 = Term(term_id=1, normalized="python", is_blocked=False)
        term2 = Term(term_id=2, normalized="プログラミング", is_blocked=False)
        term3 = Term(term_id=3, normalized="学習", is_blocked=False)
        
        def get_or_create_side_effect(normalized):
            if normalized == "python":
                return term1
            elif normalized == "プログラミング":
                return term2
            elif normalized == "学習":
                return term3
            return Term(term_id=999, normalized=normalized, is_blocked=False)
        
        mock_term_repo.get_or_create.side_effect = get_or_create_side_effect
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.fetched_threads == 1
        assert metrics.fetched_posts == 1
        assert metrics.parsed_posts == 1
        assert metrics.total_tokens == 3
        assert mock_daily_stats_repo.upsert.call_count == 3
        assert mock_metrics_repo.upsert.call_count == 1
    
    def test_process_posts_multiple_threads(self, processor, mock_noun_extractor, mock_term_repo,
                                            mock_daily_stats_repo, mock_metrics_repo):
        """複数スレッドの投稿が正しく処理される"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Pythonでプログラミング",
            ),
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000002",
                date="2025/01/01(水) 12:01:00.00",
                content="JavaScriptで開発",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        def extract_nouns_side_effect(content):
            if "Python" in content:
                return ["Python", "プログラミング"]
            elif "JavaScript" in content:
                return ["JavaScript", "開発"]
            return []
        
        mock_noun_extractor.extract_nouns.side_effect = extract_nouns_side_effect
        
        term_counter = {"count": 1}
        def get_or_create_side_effect(normalized):
            term = Term(term_id=term_counter["count"], normalized=normalized, is_blocked=False)
            term_counter["count"] += 1
            return term
        
        mock_term_repo.get_or_create.side_effect = get_or_create_side_effect
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.fetched_threads == 2
        assert metrics.fetched_posts == 2
        assert metrics.parsed_posts == 2
        assert mock_daily_stats_repo.upsert.call_count == 4  # 4つの異なる名詞
    
    def test_process_posts_post_hits_counting(self, processor, mock_noun_extractor, mock_term_repo,
                                             mock_daily_stats_repo, mock_metrics_repo):
        """同一レス内で複数回出現してもpost_hitsは1カウント"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Python Python Python",  # 同じ名詞が3回
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        mock_noun_extractor.extract_nouns.return_value = ["Python", "Python", "Python"]
        
        term = Term(term_id=1, normalized="python", is_blocked=False)
        mock_term_repo.get_or_create.return_value = term
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証: post_hitsは1（同一レス内で複数回出ても1カウント）
        assert metrics.total_tokens == 3
        upsert_calls = mock_daily_stats_repo.upsert.call_args_list
        assert len(upsert_calls) == 1
        stats = upsert_calls[0][0][0]  # 最初の引数（DailyTermStatsオブジェクト）
        assert stats.post_hits == 1
        assert stats.thread_hits == 1
    
    def test_process_posts_thread_hits_counting(self, processor, mock_noun_extractor, mock_term_repo,
                                                mock_daily_stats_repo, mock_metrics_repo):
        """同一スレッド内で複数レスに出てもthread_hitsは1カウント"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Pythonでプログラミング",
            ),
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",  # 同じスレッド
                date="2025/01/01(水) 12:01:00.00",
                content="Pythonで開発",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        def extract_nouns_side_effect(content):
            if "プログラミング" in content:
                return ["Python", "プログラミング"]
            elif "開発" in content:
                return ["Python", "開発"]
            return []
        
        mock_noun_extractor.extract_nouns.side_effect = extract_nouns_side_effect
        
        # 正規化後の文字列をキーとしてTermを管理（同じ正規化文字列には同じTermを返す）
        term_dict = {}
        term_counter = {"count": 1}
        def get_or_create_side_effect(normalized):
            if normalized not in term_dict:
                term_dict[normalized] = Term(
                    term_id=term_counter["count"],
                    normalized=normalized,
                    is_blocked=False
                )
                term_counter["count"] += 1
            return term_dict[normalized]
        
        mock_term_repo.get_or_create.side_effect = get_or_create_side_effect
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証: Pythonは2つのレスに出現するが、thread_hitsは1
        upsert_calls = mock_daily_stats_repo.upsert.call_args_list
        python_stats = None
        # "python"は正規化後の文字列なので、term_dictから取得
        python_term_id = term_dict["python"].term_id
        for call in upsert_calls:
            stats = call[0][0]
            if stats.term_id == python_term_id:
                python_stats = stats
                break
        
        assert python_stats is not None
        assert python_stats.post_hits == 2  # 2つのレスに出現
        assert python_stats.thread_hits == 1  # 1つのスレッドに出現
    
    def test_process_posts_filtered_tokens(self, processor, mock_noun_extractor, mock_term_repo,
                                          mock_daily_stats_repo, mock_metrics_repo):
        """フィルタされたトークンが正しくカウントされる"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Python a",  # "a"は1文字なのでフィルタされる
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        mock_noun_extractor.extract_nouns.return_value = ["Python", "a"]
        
        term = Term(term_id=1, normalized="python", is_blocked=False)
        mock_term_repo.get_or_create.return_value = term
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.total_tokens == 2
        assert metrics.filtered_tokens == 1  # "a"がフィルタされた
        assert metrics.filtered_rate == 0.5
    
    def test_process_posts_blocked_term(self, processor, mock_noun_extractor, mock_term_repo,
                                       mock_daily_stats_repo, mock_metrics_repo):
        """ブロックされた用語がフィルタされる"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Python blocked",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        mock_noun_extractor.extract_nouns.return_value = ["Python", "blocked"]
        
        term1 = Term(term_id=1, normalized="python", is_blocked=False)
        term2 = Term(term_id=2, normalized="blocked", is_blocked=True)  # ブロックされている
        
        def get_or_create_side_effect(normalized):
            if normalized == "python":
                return term1
            elif normalized == "blocked":
                return term2
            return Term(term_id=999, normalized=normalized, is_blocked=False)
        
        mock_term_repo.get_or_create.side_effect = get_or_create_side_effect
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.total_tokens == 2
        assert metrics.filtered_tokens == 1  # "blocked"がフィルタされた
        assert mock_daily_stats_repo.upsert.call_count == 1  # Pythonのみ保存
    
    def test_process_posts_no_nouns(self, processor, mock_noun_extractor, mock_term_repo,
                                    mock_daily_stats_repo, mock_metrics_repo):
        """名詞が抽出できない投稿が正しく処理される"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="",  # 空の投稿
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        mock_noun_extractor.extract_nouns.return_value = []
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.parsed_posts == 1
        assert metrics.total_tokens == 0
        assert mock_daily_stats_repo.upsert.call_count == 0
    
    def test_process_posts_tokenize_failure(self, processor, mock_noun_extractor, mock_term_repo,
                                            mock_daily_stats_repo, mock_metrics_repo):
        """トークン化に失敗した場合の処理"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="テスト投稿",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        
        mock_noun_extractor.extract_nouns.side_effect = Exception("MeCab error")
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key)
        
        # 検証
        assert metrics.parsed_posts == 1
        assert metrics.tokenize_fail_posts == 1
        assert mock_daily_stats_repo.upsert.call_count == 0
    
    def test_process_posts_metrics_saved(self, processor, mock_noun_extractor, mock_term_repo,
                                         mock_daily_stats_repo, mock_metrics_repo):
        """メトリクスが正しく保存される"""
        posts = [
            CollectedPost(
                thread_path="/test/read.cgi/prog/1000000001",
                date="2025/01/01(水) 12:00:00.00",
                content="Pythonでプログラミング",
            ),
        ]
        target_date = date(2025, 1, 1)
        board_key = "prog"
        run_id = uuid4()
        
        mock_noun_extractor.extract_nouns.return_value = ["Python", "プログラミング"]
        
        term_counter = {"count": 1}
        def get_or_create_side_effect(normalized):
            term = Term(term_id=term_counter["count"], normalized=normalized, is_blocked=False)
            term_counter["count"] += 1
            return term
        
        mock_term_repo.get_or_create.side_effect = get_or_create_side_effect
        mock_daily_stats_repo.upsert = Mock()
        mock_metrics_repo.upsert = Mock()
        
        # 実行
        metrics = processor.process_posts(posts, target_date, board_key, run_id=run_id)
        
        # 検証: メトリクスが保存された
        assert mock_metrics_repo.upsert.call_count == 1
        saved_metrics = mock_metrics_repo.upsert.call_args[0][0]
        assert saved_metrics.date == target_date
        assert saved_metrics.board_key == board_key
        assert saved_metrics.run_id == run_id
        assert saved_metrics.fetched_threads == 1
        assert saved_metrics.fetched_posts == 1
        assert saved_metrics.parsed_posts == 1
        assert saved_metrics.total_tokens == 2

