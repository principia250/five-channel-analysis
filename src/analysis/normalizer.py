from __future__ import annotations

import re
import unicodedata


def normalize_term(term: str) -> str:
    # 正規化ルール:
    # - Unicode正規化：NFKC
    # - 英字：小文字化
    # - 数字：全半角統一（NFKCに含まれる）
    # - 記号：原則削除（`_` / `-` / `.` / `,` / `:` / `/` / `\\` / `()` / `[]` / `!` / `?` など）
    # - 空白：連続空白を1つに→最終的にtrim、空白だけの語は捨てる
    # - 長音/波ダッシュ：`ー` / `〜` / `~` は削除
    # - 1文字語：ノイズになりやすいので除外
    if not term:
        return ""
    
    # Unicode正規化（NFKC）
    normalized = unicodedata.normalize("NFKC", term)
    
    # 英字を小文字化
    normalized = normalized.lower()
    
    # 記号を削除（`_` / `-` / `.` / `,` / `:` / `/` / `\` / `()` / `[]` / `!` / `?` など）
    # ただし、日本語の句読点（。、）は残す（必要に応じて削除対象に追加可能）
    normalized = re.sub(r'[_\-\.,:;/\\\(\)\[\]!?]', '', normalized)
    
    # 長音/波ダッシュを削除（`ー` / `〜` / `~`）
    normalized = re.sub(r'[ー〜~]', '', normalized)
    
    # 連続空白を1つに
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # 前後の空白を削除
    normalized = normalized.strip()
    
    # 1文字語は除外（空文字列を返す）
    if len(normalized) <= 1:
        return ""
    
    return normalized

