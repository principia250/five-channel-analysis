from typing import List, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
import re


@dataclass
class ThreadInfo:
    # スレッド情報を格納するデータクラス
    path: str


@dataclass
class PostInfo:
    # 投稿情報を格納するデータクラス
    date: str
    content: str


def parse_board_page(html: str) -> List[ThreadInfo]:
    soup = BeautifulSoup(html, 'lxml')
    thread_list: List[ThreadInfo] = []

    # スレッド一覧を含むdiv内の<p>タグを取得
    # background: #BEB を持つpタグ内のaタグからスレッド情報を抽出
    for p_tag in soup.find_all('p', style=re.compile(r'background:\s*#BEB')):
        a_tag = p_tag.find('a', href=re.compile(r'/test/read\.cgi/'))
        if a_tag and a_tag.get('href'):
            href = a_tag['href']
            # /l50 などのサフィックスを削除
            # /test/read.cgi/prog/1607671811/l50 -> /test/read.cgi/prog/1607671811
            path = re.sub(r'/l\d+/?$', '', href).rstrip('/')
            if path:
                thread_list.append(ThreadInfo(path=path))

    return thread_list


def parse_thread_page(html: str) -> List[PostInfo]:
    soup = BeautifulSoup(html, 'lxml')
    post_list: List[PostInfo] = []

    # class="clear post"を持つdiv要素を取得（各投稿）
    for post_div in soup.find_all('div', class_='clear post'):
        # 日付を取得
        date_span = post_div.find('span', class_='date')
        date = date_span.get_text(strip=True) if date_span else ''

        # 投稿内容を取得
        content_div = post_div.find('div', class_='post-content')
        if content_div:
            # HTMLタグを除去してテキストのみを取得
            content = content_div.get_text(separator='\n', strip=True)
        else:
            content = ''

        if date and content:
            post_list.append(PostInfo(date=date, content=content))

    return post_list

