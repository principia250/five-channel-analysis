from __future__ import annotations

from pathlib import Path
from typing import List, Optional

try:
    import MeCab
except ImportError:
    MeCab = None  # type: ignore

# NEologd辞書のインストール先（neologd_updater.pyと同じパスを使用）
from src.utils.neologd_updater import NEOLOGD_DICTIONARY_PATH


class NounExtractor:
    def __init__(self, dictionary_path: Optional[str] = None):
        if MeCab is None:
            raise ImportError(
                "mecab-python3 is not installed. Please install it with: pip install mecab-python3"
            )
        
        # 辞書パスの決定
        if dictionary_path:
            dict_path = dictionary_path
        else:
            dict_path = self._find_dictionary_path()
        
        # MeCabの初期化
        try:
            if dict_path:
                # カスタム辞書を指定（mecabrcも明示的に指定）
                mecab_args = f'-r /etc/mecabrc -d {dict_path}'
                self.tagger = MeCab.Tagger(mecab_args)
            else:
                # デフォルト設定を使用
                self.tagger = MeCab.Tagger('-r /etc/mecabrc')
        except RuntimeError as e:
            raise RuntimeError(
                "MeCab initialization failed. Make sure MeCab is installed on your system. "
                "On Ubuntu/Debian: sudo apt-get install mecab libmecab-dev mecab-ipadic-utf8"
            ) from e
    
    def _find_dictionary_path(self) -> Optional[str]:
        if NEOLOGD_DICTIONARY_PATH.exists() and (NEOLOGD_DICTIONARY_PATH / "dicrc").exists():
            return str(NEOLOGD_DICTIONARY_PATH)
        return None
    
    def extract_nouns(self, text: str) -> List[str]:
        if not text:
            return []
        
        nouns: List[str] = []
        
        try:
            # MeCabで形態素解析
            node = self.tagger.parseToNode(text)
            
            while node:
                # 品詞情報を取得
                features = node.feature.split(',')
                
                # 品詞が「名詞」で始まる場合のみ抽出
                if len(features) > 0 and features[0] == "名詞":
                    # 表層形を取得
                    surface = node.surface
                    if surface:
                        nouns.append(surface)
                
                node = node.next
        except Exception:
            # トークン化に失敗した場合は空リストを返す
            pass
        
        return nouns


def extract_nouns_from_text(text: str) -> List[str]:
    extractor = NounExtractor()
    return extractor.extract_nouns(text)

