import pytest
from src.analysis.normalizer import normalize_term


class TestNormalizeTerm:
    def test_normalize_term_basic(self):
        """基本的な正規化が動作する"""
        result = normalize_term("Python")
        assert result == "python"
    
    def test_normalize_term_unicode_normalization(self):
        """Unicode正規化（NFKC）が適用される"""
        # 全角英数字が半角に変換される
        result = normalize_term("Ｐｙｔｈｏｎ")
        assert result == "python"
    
    def test_normalize_term_lowercase(self):
        """英字が小文字化される"""
        result = normalize_term("PYTHON")
        assert result == "python"
        result = normalize_term("JavaScript")
        assert result == "javascript"
    
    def test_normalize_term_remove_symbols(self):
        """記号が削除される"""
        result = normalize_term("Python_3.10")
        assert result == "python310"
        result = normalize_term("test-case")
        assert result == "testcase"
        result = normalize_term("hello/world")
        assert result == "helloworld"
        result = normalize_term("test(example)")
        assert result == "testexample"
        result = normalize_term("array[0]")
        assert result == "array0"
    
    def test_normalize_term_remove_long_vowel(self):
        """長音/波ダッシュが削除される"""
        result = normalize_term("やばーーい")
        assert result == "やばい"
        result = normalize_term("すご〜い")
        assert result == "すごい"
        result = normalize_term("test~case")
        assert result == "testcase"
    
    def test_normalize_term_normalize_whitespace(self):
        """連続空白が正規化される"""
        result = normalize_term("hello    world")
        assert result == "hello world"
        result = normalize_term("  test  ")
        assert result == "test"
    
    def test_normalize_term_single_char_excluded(self):
        """1文字語は除外される（空文字列を返す）"""
        result = normalize_term("a")
        assert result == ""
        result = normalize_term("あ")
        assert result == ""
        result = normalize_term("1")
        assert result == ""
    
    def test_normalize_term_empty_string(self):
        """空文字列は空文字列を返す"""
        result = normalize_term("")
        assert result == ""
    
    def test_normalize_term_japanese(self):
        """日本語の名詞は正規化される"""
        result = normalize_term("プログラミング")
        assert result == "プログラミング"
        result = normalize_term("Python言語")
        assert result == "python言語"
    
    def test_normalize_term_mixed(self):
        """英数字と日本語の混在"""
        result = normalize_term("Python3.10言語")
        assert result == "python310言語"
        result = normalize_term("テスト_case")
        assert result == "テストcase"
    
    def test_normalize_term_only_symbols(self):
        """記号のみの場合は空文字列になる"""
        result = normalize_term("---")
        assert result == ""
        result = normalize_term("___")
        assert result == ""
        result = normalize_term("...")
        assert result == ""
    
    def test_normalize_term_whitespace_only(self):
        """空白のみの場合は空文字列になる"""
        result = normalize_term("   ")
        assert result == ""
        result = normalize_term("\t\n")
        assert result == ""
    
    def test_normalize_term_preserves_japanese_punctuation(self):
        """日本語の句読点は残る（現在の実装では削除されない）"""
        result = normalize_term("テスト。")
        assert result == "テスト。"
        result = normalize_term("テスト、")
        assert result == "テスト、"
    
    def test_normalize_term_numbers(self):
        """数字は正規化される（全角→半角）"""
        result = normalize_term("１２３")
        assert result == "123"
        result = normalize_term("Python3")
        assert result == "python3"

