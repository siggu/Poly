"""
은평구 보건소 메뉴 수집 Strategy
"""

from bs4 import BeautifulSoup
from typing import List, Dict, Set
from ....utils import extract_link_from_element


def collect_menu_links(soup: BeautifulSoup, start_url: str) -> List[Dict]:
    """
    은평구 메뉴에서 링크 수집

    Args:
        soup: BeautifulSoup 객체
        start_url: 시작 URL

    Returns:
        수집된 링크 목록
    """
    collected_links = []
    seen_urls = set()

    # 은평구 메뉴 선택자
    menu_selector = ".lnb ul li a"

    base_url = start_url.rsplit("/", 1)[0]

    menu_links = soup.select(menu_selector)

    for link_element in menu_links:
        link_info = extract_link_from_element(link_element, base_url, seen_urls)
        if link_info:
            collected_links.append(link_info)

    print(f"  [은평구 Strategy] {len(collected_links)}개 링크 수집")
    return collected_links
