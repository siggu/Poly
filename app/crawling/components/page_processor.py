"""
페이지 처리 전담 클래스
페이지 내 탭 메뉴 탐지 및 제목 결정을 수행합니다.
"""

from typing import List, Dict
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
import utils


class PageProcessor:
    """페이지 처리 전담 클래스"""

    def __init__(self):
        pass

    def find_tabs_on_page(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """
        페이지에서 탭 메뉴 찾기

        Args:
            soup: BeautifulSoup 객체
            url: 현재 페이지 URL

        Returns:
            탭 링크 목록 [{"name": str, "url": str}, ...]
        """
        tab_selectors = config.TAB_SELECTORS
        tab_links = []

        for tab_selector in tab_selectors:
            tab_elements = soup.select(tab_selector)
            if tab_elements:
                for tab_link_element in tab_elements:
                    # 탭의 경우 현재 페이지 URL을 base로 사용 (href="#" 처리 위해)
                    link_info = self._extract_link_from_element(
                        tab_link_element,
                        url,  # base_url 대신 전체 URL 사용
                        set(),  # 중복 검사는 나중에
                    )
                    if link_info:
                        tab_links.append(link_info)

                if tab_links:
                    print(
                        f"    -> 탭 메뉴 발견 ({len(tab_links)}개 항목, 선택자: '{tab_selector}')"
                    )
                    break  # 첫 번째로 찾은 선택자 사용

        return tab_links

    def determine_page_title(
        self, name: str, url: str, tab_links: List[Dict]
    ) -> str:
        """
        페이지의 정확한 제목 결정 (탭이 있는 경우 매칭)

        Args:
            name: 기본 제목
            url: 현재 페이지 URL
            tab_links: 탭 링크 목록

        Returns:
            최종 제목
        """
        if not tab_links:
            return name

        # 현재 URL과 일치하는 탭 찾기
        for tab_info in tab_links:
            if utils.are_urls_equivalent(tab_info["url"], url):
                print(
                    f"    -> 현재 페이지는 '{tab_info['name']}' 탭이므로 제목 업데이트"
                )
                return tab_info["name"]

        # URL 매칭 실패 시, 첫 번째 탭을 기본 페이지로 간주
        if tab_links:
            print(
                f"    -> URL 매칭 실패. 첫 번째 탭 '{tab_links[0]['name']}'을 현재 페이지로 간주"
            )
            return tab_links[0]["name"]

        return name

    def _extract_link_from_element(
        self, link_element, base_url: str, seen_urls: set
    ) -> dict:
        """
        링크 요소에서 URL과 이름을 추출하고 검증

        Args:
            link_element: BeautifulSoup 링크 요소
            base_url: 기준 URL
            seen_urls: 이미 수집된 URL 집합

        Returns:
            {"name": str, "url": str} 또는 None (무효한 링크인 경우)
        """
        name = link_element.get_text(strip=True)
        href = link_element.get("href", "")

        if not href:
            return None

        # 절대 URL로 변환
        url = urljoin(base_url, href)

        # 중복 확인
        if url in seen_urls:
            return None

        return {"name": name, "url": url}
