"""
링크 수집 전담 클래스
보건소 사이트의 LNB 또는 단일 페이지에서 링크를 수집합니다.
"""

import time
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
import utils
from utils import extract_link_from_element
from base.base_crawler import BaseCrawler


class LinkCollector(BaseCrawler):
    """링크 수집 전담 클래스"""

    def __init__(self):
        super().__init__()

    def collect_links(self, start_url: str, crawl_rules: List[Dict]) -> List[Dict]:
        """
        초기 링크 목록 수집

        Args:
            start_url: 시작 URL
            crawl_rules: 크롤링 규칙 목록

        Returns:
            수집된 링크 목록 [{"name": str, "url": str}, ...]
        """
        base_url = utils.get_base_url(start_url)

        # 페이지 가져오기
        soup = self.fetch_page(start_url)
        if not soup:
            print(f"오류: 시작 URL({start_url})에 접근할 수 없습니다.")
            return []

        # 적용할 규칙 찾기
        active_rule, main_links_elements = self._find_applicable_rule(
            soup, start_url, crawl_rules
        )

        if not active_rule:
            print("경고: 적용 가능한 크롤링 규칙을 찾지 못했습니다. 빈 목록을 반환합니다.")
            return []

        # 규칙에 따라 링크 수집
        if active_rule.get("single_page", False):
            collected_links = self._collect_single_page_links(
                main_links_elements, base_url, active_rule
            )
        else:
            collected_links = self._collect_lnb_links(
                main_links_elements, base_url, active_rule
            )

        # 최종 중복 제거
        return self._deduplicate_links(collected_links)

    def _find_applicable_rule(
        self, soup: BeautifulSoup, start_url: str, crawl_rules: List[Dict]
    ) -> tuple:
        """
        적용 가능한 크롤링 규칙 찾기

        Returns:
            (active_rule, main_links_elements) 튜플
        """
        for rule in crawl_rules:
            if "domain" in rule and rule["domain"].lower() not in start_url.lower():
                continue

            if rule.get("single_page", False):
                main_links_elements = self._process_single_page_rule(soup, rule)
                if main_links_elements:
                    return rule, main_links_elements
            else:
                main_links_elements = soup.select(rule["main_selector"])
                if main_links_elements:
                    print(
                        f"  [OK] 규칙 적용: '{rule['name']}' ({len(main_links_elements)}개 링크 발견)"
                    )
                    return rule, main_links_elements

        return None, []

    def _process_single_page_rule(
        self, soup: BeautifulSoup, rule: Dict
    ) -> List:
        """
        single_page 규칙 처리

        Returns:
            main_links_elements 또는 빈 리스트
        """
        menu_container = soup.select_one(rule.get("menu_container", "body"))
        if not menu_container:
            return []

        found_menu_scope = menu_container

        # filter_menu가 있으면 특정 메뉴 필터링
        if filter_menu := rule.get("filter_menu"):
            potential_parents = menu_container.select("li:has(> a)")
            matched_parent = None

            for item in potential_parents:
                link = item.find("a", recursive=False)
                if link and filter_menu in link.get_text(strip=True):
                    matched_parent = item
                    break

            if matched_parent:
                found_menu_scope = matched_parent
            else:
                print(
                    f"  경고: single_page 규칙 '{rule['name']}'에서 filter_menu '{filter_menu}'를 찾지 못함. 전체 컨테이너 탐색."
                )

        # 링크 탐색
        main_links_elements = found_menu_scope.select(rule["main_selector"])

        if main_links_elements:
            log_msg = f"'{rule['name']}' ({len(main_links_elements)}개 링크 후보 발견 - single_page"
            if filter_menu:
                log_msg += f", filter: '{filter_menu}'"
            log_msg += ")"
            print(f"  [OK] 규칙 적용: {log_msg}")

        return main_links_elements

    def _collect_single_page_links(
        self, main_links_elements: List, base_url: str, active_rule: Dict
    ) -> List[Dict]:
        """
        single_page 모드 링크 수집

        Returns:
            수집된 링크 목록
        """
        collected_links = []
        seen_urls = set()

        sub_selector = active_rule.get("sub_selector")

        if sub_selector:
            # sub_selector가 있으면 계층 구조로 처리
            collected_links = self._collect_hierarchical_links(
                main_links_elements, base_url, sub_selector, seen_urls
            )
        else:
            # sub_selector 없으면 main_links_elements가 최종 링크
            for link_element in main_links_elements:
                link_info = extract_link_from_element(
                    link_element, base_url, seen_urls
                )
                if link_info:
                    seen_urls.add(link_info["url"])
                    collected_links.append(link_info)

        print(f"  [OK] 총 {len(collected_links)}개 링크 수집 (single_page, 중복 제거)")
        return collected_links

    def _collect_hierarchical_links(
        self, main_links_elements: List, base_url: str, sub_selector, seen_urls: set
    ) -> List[Dict]:
        """
        계층 구조 링크 수집 (depth1 -> depth2/depth3)

        Returns:
            수집된 링크 목록
        """
        collected_links = []

        # sub_selector를 리스트로 변환
        sub_selectors = (
            [sub_selector] if isinstance(sub_selector, str) else sub_selector
        )

        for depth1_element in main_links_elements:
            parent_element = depth1_element.find_parent("li") or depth1_element

            # 여러 선택자 시도
            sub_link_elements = []
            for selector in sub_selectors:
                elements = parent_element.select(selector)
                if elements:
                    sub_link_elements.extend(elements)
                    break

            # 하위 메뉴 없으면 depth1 자체가 링크인지 확인
            if not sub_link_elements:
                if depth1_element.name == "a":
                    sub_link_elements = [depth1_element]
                else:
                    continue

            # 링크 추출
            for link_element in sub_link_elements:
                link_info = extract_link_from_element(
                    link_element, base_url, seen_urls
                )
                if link_info:
                    seen_urls.add(link_info["url"])
                    collected_links.append(link_info)

        return collected_links

    def _collect_lnb_links(
        self, main_links_elements: List, base_url: str, active_rule: Dict
    ) -> List[Dict]:
        """
        LNB 모드 링크 수집 (각 카테고리 방문)

        Returns:
            수집된 링크 목록
        """
        collected_links = []
        seen_urls = set()
        main_categories = []

        filter_menu = active_rule.get("filter_menu")
        if filter_menu:
            print(f"  [INFO] 필터링 적용: '{filter_menu}' 포함 메뉴만 수집")

        # 메인 카테고리 수집
        for link_element in main_links_elements:
            name = link_element.get_text(strip=True)

            if filter_menu and filter_menu not in name:
                continue

            link_info = extract_link_from_element(
                link_element, base_url, seen_urls
            )
            if link_info:
                main_categories.append(link_info)

        # 각 카테고리 방문하여 하위 메뉴 수집
        for category in main_categories:
            if category["url"] in seen_urls:
                print(f"\n  LNB 하위 탐색 건너뜀 (이미 처리됨): {category['name']}")
                continue

            print(f"\n  LNB 하위 탐색: {category['name']}")
            time.sleep(config.RATE_LIMIT_DELAY)

            try:
                cat_soup = self.fetch_page(category["url"])
                if not cat_soup:
                    raise ValueError(f"페이지를 가져올 수 없습니다: {category['url']}")

                sub_links = self._extract_sub_links(cat_soup, base_url, active_rule)

                if sub_links:
                    print(f"    -> 하위 메뉴 {len(sub_links)}개 발견")
                    for link_info in sub_links:
                        if link_info["url"] not in seen_urls:
                            seen_urls.add(link_info["url"])
                            collected_links.append(link_info)
                else:
                    print("    -> 하위 메뉴 없음 (또는 sub_selector 없음), 카테고리 자체 추가")
                    if category["url"] not in seen_urls:
                        seen_urls.add(category["url"])
                        collected_links.append(category)

            except requests.RequestException as e:
                print(f"    ✗ 오류: {category['url']} 방문 실패 - {e}")
            except Exception as e:
                print(f"    ✗ 오류: {category['url']} 처리 중 예외 발생 - {e}")

        return collected_links

    def _extract_sub_links(
        self, soup: BeautifulSoup, base_url: str, active_rule: Dict
    ) -> List[Dict]:
        """
        하위 링크 추출

        Returns:
            추출된 링크 목록
        """
        sub_link_elements = []
        sub_selectors = active_rule.get("sub_selector", [])

        if isinstance(sub_selectors, str):
            sub_selectors = [sub_selectors]

        if not sub_selectors:
            return []

        # 선택자로 하위 링크 찾기
        for selector in sub_selectors:
            elements = soup.select(selector)
            if elements:
                sub_link_elements.extend(elements)
                break

        # 링크 추출
        extracted_links = []
        seen_urls = set()
        for link_element in sub_link_elements:
            link_info = extract_link_from_element(
                link_element, base_url, seen_urls
            )
            if link_info:
                seen_urls.add(link_info["url"])
                extracted_links.append(link_info)

        return extracted_links

    def _deduplicate_links(self, links: List[Dict]) -> List[Dict]:
        """
        링크 목록 중복 제거

        Args:
            links: 링크 목록

        Returns:
            중복이 제거된 링크 목록
        """
        final_links = []
        final_seen_urls = set()

        for link in links:
            if link["url"] not in final_seen_urls:
                final_links.append(link)
                final_seen_urls.add(link["url"])

        return final_links
