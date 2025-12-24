import pytest
from unittest.mock import Mock, patch, MagicMock

from src.analysis.noun_extractor import NounExtractor, extract_nouns_from_text


class TestNounExtractorInit:
    @patch('src.analysis.noun_extractor.MeCab')
    def test_init_success(self, mock_mecab):
        """正常に初期化できる"""
        mock_tagger = Mock()
        mock_mecab.Tagger.return_value = mock_tagger
        
        extractor = NounExtractor()
        
        assert extractor.tagger == mock_tagger
        mock_mecab.Tagger.assert_called_once()
    
    def test_init_import_error(self):
        """MeCabがインストールされていない場合にImportErrorが発生する"""
        with patch('src.analysis.noun_extractor.MeCab', None):
            with pytest.raises(ImportError, match="mecab-python3 is not installed"):
                NounExtractor()
    
    @patch('src.analysis.noun_extractor.MeCab')
    def test_init_runtime_error(self, mock_mecab):
        """MeCabの初期化に失敗した場合にRuntimeErrorが発生する"""
        mock_mecab.Tagger.side_effect = RuntimeError("MeCab initialization failed")
        
        with pytest.raises(RuntimeError, match="MeCab initialization failed"):
            NounExtractor()


class TestNounExtractorExtractNouns:
    """NounExtractor.extract_nouns()のテスト"""
    
    @pytest.fixture
    def mock_tagger(self):
        """MeCab Taggerのモック"""
        return Mock()
    
    @pytest.fixture
    def extractor(self, mock_tagger):
        """NounExtractorのインスタンス"""
        with patch('src.analysis.noun_extractor.MeCab') as mock_mecab:
            mock_mecab.Tagger.return_value = mock_tagger
            return NounExtractor()
    
    def test_extract_nouns_empty_string(self, extractor, mock_tagger):
        """空文字列の場合は空リストを返す"""
        result = extractor.extract_nouns("")
        assert result == []
        mock_tagger.parseToNode.assert_not_called()
    
    def test_extract_nouns_success(self, extractor, mock_tagger):
        """名詞が正しく抽出される"""
        # モックノードを作成
        node1 = Mock()
        node1.feature = "名詞,一般,*,*,*,*,*"
        node1.surface = "Python"
        
        node2 = Mock()
        node2.feature = "動詞,自立,*,*,*,*,*"
        node2.surface = "実行"
        
        node3 = Mock()
        node3.feature = "名詞,一般,*,*,*,*,*"
        node3.surface = "プログラム"
        
        node4 = Mock()
        node4.feature = "BOS/EOS,*,*,*,*,*,*"
        node4.surface = ""
        
        # ノードチェーンを構築
        node1.next = node2
        node2.next = node3
        node3.next = node4
        node4.next = None
        
        mock_tagger.parseToNode.return_value = node1
        
        result = extractor.extract_nouns("Pythonでプログラムを実行")
        
        assert result == ["Python", "プログラム"]
        mock_tagger.parseToNode.assert_called_once_with("Pythonでプログラムを実行")
    
    def test_extract_nouns_no_nouns(self, extractor, mock_tagger):
        """名詞が含まれていない場合は空リストを返す"""
        node1 = Mock()
        node1.feature = "動詞,自立,*,*,*,*,*"
        node1.surface = "実行"
        
        node2 = Mock()
        node2.feature = "BOS/EOS,*,*,*,*,*,*"
        node2.surface = ""
        
        node1.next = node2
        node2.next = None
        
        mock_tagger.parseToNode.return_value = node1
        
        result = extractor.extract_nouns("実行する")
        
        assert result == []
    
    def test_extract_nouns_various_noun_types(self, extractor, mock_tagger):
        """様々な名詞の種類が抽出される"""
        node1 = Mock()
        node1.feature = "名詞,一般,*,*,*,*,*"
        node1.surface = "Python"
        
        node2 = Mock()
        node2.feature = "名詞,固有名詞,*,*,*,*,*"
        node2.surface = "東京"
        
        node3 = Mock()
        node3.feature = "名詞,サ変接続,*,*,*,*,*"
        node3.surface = "プログラミング"
        
        node4 = Mock()
        node4.feature = "BOS/EOS,*,*,*,*,*,*"
        node4.surface = ""
        
        node1.next = node2
        node2.next = node3
        node3.next = node4
        node4.next = None
        
        mock_tagger.parseToNode.return_value = node1
        
        result = extractor.extract_nouns("Python東京プログラミング")
        
        assert result == ["Python", "東京", "プログラミング"]
    
    def test_extract_nouns_empty_surface(self, extractor, mock_tagger):
        """表層形が空の名詞は除外される"""
        node1 = Mock()
        node1.feature = "名詞,一般,*,*,*,*,*"
        node1.surface = "Python"
        
        node2 = Mock()
        node2.feature = "名詞,一般,*,*,*,*,*"
        node2.surface = ""  # 空の表層形
        
        node3 = Mock()
        node3.feature = "BOS/EOS,*,*,*,*,*,*"
        node3.surface = ""
        
        node1.next = node2
        node2.next = node3
        node3.next = None
        
        mock_tagger.parseToNode.return_value = node1
        
        result = extractor.extract_nouns("Python")
        
        assert result == ["Python"]
    
    def test_extract_nouns_exception_handling(self, extractor, mock_tagger):
        """例外が発生した場合は空リストを返す"""
        mock_tagger.parseToNode.side_effect = Exception("MeCab error")
        
        result = extractor.extract_nouns("テスト")
        
        assert result == []
    
    def test_extract_nouns_invalid_feature(self, extractor, mock_tagger):
        """featureが空の場合はスキップされる"""
        node1 = Mock()
        node1.feature = ""  # 空のfeature
        node1.surface = "test"
        
        node2 = Mock()
        node2.feature = "名詞,一般,*,*,*,*,*"
        node2.surface = "Python"
        
        node3 = Mock()
        node3.feature = "BOS/EOS,*,*,*,*,*,*"
        node3.surface = ""
        
        node1.next = node2
        node2.next = node3
        node3.next = None
        
        mock_tagger.parseToNode.return_value = node1
        
        result = extractor.extract_nouns("test Python")
        
        assert result == ["Python"]


class TestExtractNounsFromText:
    """extract_nouns_from_text()のテスト"""
    
    @patch('src.analysis.noun_extractor.NounExtractor')
    def test_extract_nouns_from_text(self, mock_extractor_class):
        """簡易関数が正しく動作する"""
        mock_extractor = Mock()
        mock_extractor.extract_nouns.return_value = ["Python", "プログラム"]
        mock_extractor_class.return_value = mock_extractor
        
        result = extract_nouns_from_text("Pythonでプログラムを実行")
        
        assert result == ["Python", "プログラム"]
        mock_extractor_class.assert_called_once()
        mock_extractor.extract_nouns.assert_called_once_with("Pythonでプログラムを実行")

