"""
베이스 Strategy

모든 구별 strategy가 따라야 하는 인터페이스
"""

from bs4 import BeautifulSoup
from typing import List, Dict


def collect_menu_links(soup: BeautifulSoup, start_url: str) -> List[Dict]:
    """
    메뉴에서 링크 수집 (인터페이스)

    Args:
        soup: BeautifulSoup 객체
        start_url: 시작 URL

    Returns:
        수집된 링크 목록 [{"name": str, "url": str}, ...]
    """
    raise NotImplementedError("각 구별 strategy에서 구현해야 합니다.")
