"""
워크플로우: 링크 수집 -> 크롤링 및 구조화 (탭 처리 포함 - 컨테이너 페이지 저장)

1. 초기 링크 수집: 보건소 사이트의 LNB 등에서 서브 메뉴 링크 수집
2. 링크 처리 루프:
   - 각 링크 페이지 방문
   - 페이지 내부에 탭 메뉴가 있는지 확인
   - ★★★ 현재 페이지 내용을 LLM으로 구조화 (탭 유무와 상관없이 항상 실행) ★★★
   - 탭 발견 시: 새로운 탭 링크들을 처리 목록에 추가
3. 모든 결과를 JSON 파일로 저장.
"""

import json
import os
from datetime import datetime
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

# 공통 모듈 import
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
import utils
from base.base_crawler import BaseCrawler
from base.llm_crawler import LLMStructuredCrawler


class HealthCareWorkflow(BaseCrawler):
    """보건소 사이트 크롤링 및 구조화 워크플로우 (탭 처리 기능 포함 - 컨테이너 저장)"""

    def __init__(
        self, output_dir: str = "app/interface/crawling/output", region: str = None
    ):
        """
        Args:
            output_dir: 결과 저장 디렉토리
            region: 지역명 (예: "동작구"). None이면 URL에서 자동 추출 시도
        """
        super().__init__()  # BaseCrawler 초기화
        self.output_dir = output_dir
        self.region = region
        # LLM 크롤러 초기화 시 모델 지정
        self.crawler = LLMStructuredCrawler(model="gpt-4o-mini")  # 또는 "gpt-4o" 등

        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)

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

        # 기본 검사
        if not href:
            return None

        # 절대 URL로 변환
        url = urljoin(base_url, href)

        # 중복 확인
        if url in seen_urls:
            return None

        return {"name": name, "url": url}

    def _find_tabs_on_page(self, soup: BeautifulSoup, url: str) -> List[Dict]:
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

    def _determine_page_title(self, name: str, url: str, tab_links: List[Dict]) -> str:
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

    def collect_links(self, start_url: str, crawl_rules: List[Dict]) -> List[Dict]:
        """
        초기 링크 목록 수집 (LNB 등 - 이전과 동일한 로직)
        """
        base_url = utils.get_base_url(start_url)

        # 페이지 가져오기 (BaseCrawler의 fetch_page 사용)
        soup = self.fetch_page(start_url)
        if not soup:
            print(f"오류: 시작 URL({start_url})에 접근할 수 없습니다.")
            return []

        # 적용할 규칙 찾기
        main_links_elements = []
        active_rule = None
        for rule in crawl_rules:
            if "domain" in rule and rule["domain"].lower() not in start_url.lower():
                continue

            # single_page 모드 처리
            if rule.get("single_page", False):
                menu_container = soup.select_one(rule.get("menu_container", "body"))
                if not menu_container:
                    continue

                found_menu_scope = menu_container  # 기본 탐색 범위
                if filter_menu := rule.get("filter_menu"):
                    # 특정 메뉴 필터링 로직 강화
                    potential_parents = menu_container.select(
                        "li:has(> a)"
                    )  # a를 직접 가진 li 탐색
                    matched_parent = None
                    for item in potential_parents:
                        link = item.find("a", recursive=False)
                        if link and filter_menu in link.get_text(strip=True):
                            matched_parent = item  # 필터링된 메뉴의 li 찾음
                            break
                    if matched_parent:
                        found_menu_scope = (
                            matched_parent  # 탐색 범위를 필터링된 메뉴로 좁힘
                        )
                    else:
                        print(
                            f"  경고: single_page 규칙 '{rule['name']}'에서 filter_menu '{filter_menu}'를 찾지 못함. 전체 컨테이너 탐색."
                        )
                        # 못 찾으면 전체 컨테이너에서 계속 진행 (또는 continue로 다음 규칙 시도)

                # 결정된 범위(found_menu_scope) 내에서 main_selector로 링크 탐색
                main_links_elements = found_menu_scope.select(rule["main_selector"])

                if main_links_elements:
                    log_msg = f"'{rule['name']}' ({len(main_links_elements)}개 링크 후보 발견 - single_page"
                    if filter_menu:
                        log_msg += f", filter: '{filter_menu}'"
                    log_msg += ")"
                    print(f"  [OK] 규칙 적용: {log_msg}")
                    active_rule = rule
                    break  # 규칙 찾으면 종료

            else:  # 일반 LNB 모드
                main_links_elements = soup.select(rule["main_selector"])
                if main_links_elements:
                    print(
                        f"  [OK] 규칙 적용: '{rule['name']}' ({len(main_links_elements)}개 링크 발견)"
                    )
                    active_rule = rule
                    break

        if not active_rule:
            print(
                "경고: 적용 가능한 크롤링 규칙을 찾지 못했습니다. 빈 목록을 반환합니다."
            )
            return []

        # 링크 추출 및 반환
        collected_links = []
        seen_urls = set()

        # single_page 모드 링크 처리
        if active_rule.get("single_page", False):
            # sub_selector가 있으면 계층 구조로 처리
            if sub_selector := active_rule.get("sub_selector"):
                # sub_selector가 리스트가 아니면 리스트로 변환
                if isinstance(sub_selector, str):
                    sub_selectors = [sub_selector]
                else:
                    sub_selectors = sub_selector

                for depth1_element in main_links_elements:
                    # depth1_element 자체가 링크일 수도 있고, 아닐 수도 있음. 유연하게 처리
                    parent_element = (
                        depth1_element.find_parent("li") or depth1_element
                    )  # li가 없으면 자기 자신

                    # 여러 선택자 시도
                    sub_link_elements = []
                    for selector in sub_selectors:
                        elements = parent_element.select(selector)
                        if elements:
                            sub_link_elements.extend(elements)
                            break  # 첫 번째로 찾은 선택자 사용

                    if (
                        not sub_link_elements
                    ):  # 하위 메뉴 없으면 1depth 자체가 링크인지 확인
                        if depth1_element.name == "a":
                            sub_link_elements = [depth1_element]
                        else:  # a 태그가 아니면 건너뜀
                            continue

                    # 링크 추출 (헬퍼 메서드 사용)
                    for link_element in sub_link_elements:
                        link_info = self._extract_link_from_element(
                            link_element, base_url, seen_urls
                        )
                        if link_info:
                            seen_urls.add(link_info["url"])
                            collected_links.append(link_info)
            else:  # sub_selector 없으면 main_links_elements가 최종 링크
                for link_element in main_links_elements:
                    link_info = self._extract_link_from_element(
                        link_element, base_url, seen_urls
                    )
                    if link_info:
                        seen_urls.add(link_info["url"])
                        collected_links.append(link_info)
            print(
                f"  [OK] 총 {len(collected_links)}개 링크 수집 (single_page, 중복 제거)"
            )

        # 일반 LNB 모드 링크 처리
        else:
            main_categories = []
            filter_menu = active_rule.get("filter_menu")  # filter_menu 가져오기

            if filter_menu:
                print(f"  [INFO] 필터링 적용: '{filter_menu}' 포함 메뉴만 수집")

            for link_element in main_links_elements:
                name = link_element.get_text(strip=True)

                # filter_menu가 있으면 필터링
                if filter_menu and filter_menu not in name:
                    continue

                link_info = self._extract_link_from_element(
                    link_element, base_url, seen_urls
                )
                if link_info:
                    main_categories.append(link_info)

            # 각 카테고리 방문하여 하위 메뉴 수집
            for category in main_categories:
                # 이미 처리된 URL이면 건너뛰기 (중복 방지 강화)
                if category["url"] in seen_urls:
                    print(f"\n  LNB 하위 탐색 건너뜀 (이미 처리됨): {category['name']}")
                    continue

                print(f"\n  LNB 하위 탐색: {category['name']}")
                time.sleep(config.RATE_LIMIT_DELAY)  # Rate limiting

                try:
                    # fetch_page 사용
                    cat_soup = self.fetch_page(category["url"])
                    if not cat_soup:
                        raise ValueError(
                            f"페이지를 가져올 수 없습니다: {category['url']}"
                        )

                    sub_link_elements = []
                    sub_selectors = active_rule.get("sub_selector", [])
                    if isinstance(sub_selectors, str):
                        sub_selectors = [sub_selectors]

                    found_sub_links = False
                    if sub_selectors:  # sub_selector가 정의된 경우에만 탐색
                        for selector in sub_selectors:
                            elements = cat_soup.select(selector)
                            if elements:
                                sub_link_elements.extend(elements)
                                found_sub_links = True  # 하나라도 찾으면 True

                    if found_sub_links:
                        print(f"    -> 하위 메뉴 {len(sub_link_elements)}개 발견")
                        for link_element in sub_link_elements:
                            link_info = self._extract_link_from_element(
                                link_element, base_url, seen_urls
                            )
                            if link_info:
                                seen_urls.add(link_info["url"])
                                collected_links.append(link_info)
                    else:  # sub_selector가 없거나, 있어도 못 찾은 경우
                        print(
                            "    -> 하위 메뉴 없음 (또는 sub_selector 없음), 카테고리 자체 추가"
                        )
                        url = category["url"]
                        if url not in seen_urls:
                            seen_urls.add(url)
                            collected_links.append(
                                {"name": category["name"], "url": url}
                            )

                except requests.RequestException as e:
                    print(f"    ✗ 오류: {category['url']} 방문 실패 - {e}")
                except Exception as e:
                    print(f"    ✗ 오류: {category['url']} 처리 중 예외 발생 - {e}")

        # 최종 반환 전 한번 더 중복 제거 (안전 장치)
        final_links = []
        final_seen_urls = set()
        for link in collected_links:
            if link["url"] not in final_seen_urls:
                final_links.append(link)
                final_seen_urls.add(link["url"])

        return final_links

    def run(
        self,
        start_url: str,
        crawl_rules: List[Dict] = None,
        save_links: bool = True,
    ) -> Dict:
        """
        전체 워크플로우 실행 (탭 처리 로직 수정: 컨테이너 페이지 저장)
        """
        print("=" * 80)
        print("보건소 사이트 크롤링 워크플로우 시작")
        print("=" * 80)

        # 기본 크롤링 규칙 (config.py에서 가져오기)
        if crawl_rules is None:
            crawl_rules = config.CRAWL_RULES

        # 1단계: 초기 링크 수집
        print("\n[1단계] 초기 링크 수집 중...")
        print("-" * 80)
        initial_links = self.collect_links(start_url, crawl_rules)
        print(f"\n[SUCCESS] 총 {len(initial_links)}개의 초기 링크 수집 완료")

        if not initial_links:
            print("처리할 링크가 없습니다. 워크플로우를 종료합니다.")
            return {}

        # 링크 저장 (초기 링크만)
        if save_links:
            links_file = os.path.join(self.output_dir, "collected_initial_links.json")
            try:
                with open(links_file, "w", encoding="utf-8") as f:
                    json.dump(initial_links, f, ensure_ascii=False, indent=2)
                print(f"[FILE] 초기 링크 목록 저장: {links_file}")
            except IOError as e:
                print(f"경고: 초기 링크 파일 저장 실패 - {e}")

        # 2단계: 링크 처리 루프 (탭 링크 포함)
        print("\n[2단계] 페이지 처리 및 LLM 구조화 (탭 포함)...")
        print("-" * 80)

        structured_data_list = []
        failed_urls = []
        links_to_process = list(initial_links)  # 처리할 링크 목록 (큐)
        processed_or_queued_urls: List[str] = [
            link["url"] for link in initial_links
        ]  # 중복 방지 List

        # 탭 메뉴를 찾는 데 사용할 CSS 선택자 목록 (config.py에서 가져오기)
        tab_selectors = config.TAB_SELECTORS

        processed_count = 0

        # while 루프로 변경하여 동적으로 추가되는 탭 링크 처리
        while links_to_process:
            link_info = links_to_process.pop(0)  # 큐에서 링크 가져오기
            url = link_info["url"]
            name = link_info["name"]
            processed_count += 1
            total_links_estimate = len(processed_or_queued_urls)

            print(f"\n[{processed_count}/{total_links_estimate}*] 처리 시도: {name}")
            print(f"  URL: {url}")
            time.sleep(1)  # 부하 감소 지연

            try:
                print("    [디버그] >> 처리 시작")
                # 1. 페이지 가져오기
                soup = self.crawler.fetch_page(url)
                if not soup:
                    raise ValueError("페이지 내용을 가져올 수 없습니다.")

                # 2. 탭 메뉴 확인 (헬퍼 메서드 사용)
                tab_links_on_page = self._find_tabs_on_page(soup, url)
                found_tabs = bool(tab_links_on_page)

                # 탭 발견 시 현재 페이지의 정확한 제목 찾기 (헬퍼 메서드 사용)
                title_for_llm = self._determine_page_title(name, url, tab_links_on_page)

                # ★★★ 3. 현재 페이지 LLM 구조화 (탭 유무와 상관없이 실행) ★★★
                print("    -> 내용 구조화 진행...")
                region = self.region or utils.extract_region_from_url(url)
                # LLM 호출 시 수집된 name을 title로 명확히 전달
                structured_data = self.crawler.crawl_and_structure(
                    url=url,  # crawler 내부에서 fetch 또는 soup 처리
                    region=region,
                    title=title_for_llm,
                )
                # 결과 리스트에 추가
                structured_data_list.append(structured_data.model_dump())
                print("  [SUCCESS] 성공")  # 일단 현재 페이지 처리 성공 로그

                # 4. 탭 발견 시, 새로운 탭 링크만 큐에 추가 (URL 중복 검사 강화)
                if found_tabs:
                    newly_added_count = 0
                    for tab_link_info in tab_links_on_page:
                        # utils.are_urls_equivalent를 사용하여 이미 큐에 있거나 처리된 URL인지 확인
                        is_already_processed = any(
                            utils.are_urls_equivalent(
                                existing_url, tab_link_info["url"]
                            )
                            for existing_url in processed_or_queued_urls
                        )

                        if not is_already_processed:
                            links_to_process.append(tab_link_info)
                            processed_or_queued_urls.append(
                                tab_link_info["url"]
                            )  # 큐에 추가되었음을 기록
                            newly_added_count += 1
                            print(
                                f"      + 탭 링크 추가: {tab_link_info['name']} ({tab_link_info['url']})"
                            )
                    if newly_added_count > 0:
                        print(
                            f"    -> 새로운 탭 링크 {newly_added_count}개를 처리 목록에 추가했습니다."
                        )
                print("    [디버그] >> 처리 완료")
            # else:
            # 이미 추가된 링크에 대한 로그는 불필요하므로 제거

            except Exception as e:
                print(f"  [ERROR] 실패: {e}")
                # 실패 시 상세 정보 기록
                import traceback

                error_details = traceback.format_exc()
                failed_urls.append(
                    {
                        "url": url,
                        "name": name,
                        "error": str(e),
                        "details": error_details,
                    }
                )
                print(f"  오류 상세:\n{error_details}")  # 콘솔에도 상세 오류 출력

        # 3단계: 결과 저장
        print("\n[3단계] 결과 저장 중...")
        print("-" * 80)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 지역명은 초기 URL 기준 또는 지정된 값 사용
        region_name = self.region or utils.extract_region_from_url(start_url)

        # 전체 구조화 데이터 저장
        output_file = os.path.join(
            self.output_dir, f"structured_data_{region_name}.json"
        )
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(structured_data_list, f, ensure_ascii=False, indent=2)
            print(f"[SUCCESS] 구조화 데이터 저장: {output_file}")
        except IOError as e:
            print(f"오류: 구조화 데이터 파일 저장 실패 - {e}")

        # 실패한 URL 저장
        failed_file = None  # 초기화
        if failed_urls:
            failed_file = os.path.join(
                self.output_dir, f"failed_urls_{region_name}.json"
            )
            try:
                with open(failed_file, "w", encoding="utf-8") as f:
                    # 실패 정보에 상세 오류(details) 포함하여 저장
                    json.dump(failed_urls, f, ensure_ascii=False, indent=2)
                print(f"[WARNING]  실패한 URL 저장: {failed_file}")
            except IOError as e:
                print(f"경고: 실패한 URL 파일 저장 실패 - {e}")

        # 요약 정보
        final_successful_count = len(structured_data_list)
        final_failed_count = len(failed_urls)
        # 총 처리 시도 횟수는 processed_count 사용
        summary = {
            "timestamp": timestamp,
            "region": region_name,
            "start_url": start_url,
            "initial_links_collected": len(initial_links),
            "total_urls_processed_or_failed": processed_count,
            "successful_structured": final_successful_count,
            "failed_processing": final_failed_count,
            "output_file": output_file,
            "failed_urls_file": failed_file,  # 실패 파일 경로 저장
        }

        summary_file = os.path.join(self.output_dir, f"summary_{timestamp}.json")
        try:
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            print(f"[FILE] 요약 정보 저장: {summary_file}")
        except IOError as e:
            print(f"경고: 요약 파일 저장 실패 - {e}")

        # 최종 요약 출력
        print("\n" + "=" * 80)
        print("워크플로우 완료")
        print("=" * 80)
        print(f"[STAT] 초기 수집 링크 수: {len(initial_links)}")
        print(f"[STAT] 총 처리 시도 URL 수: {processed_count}")
        print(f"[SUCCESS] 성공 (구조화): {final_successful_count}개")
        print(f"[ERROR] 실패: {final_failed_count}개")
        print(f"[DIR] 결과 저장 위치: {self.output_dir}")
        print("=" * 80)

        return summary


def main():
    """메인 실행 함수 (이전과 동일)"""
    import argparse

    parser = argparse.ArgumentParser(
        description="보건소 사이트 크롤링 및 구조화 워크플로우"
    )
    parser.add_argument("--url", type=str, help="시작 URL (보건소 보건사업 페이지)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="app/interface/crawling/output",
        help="결과를 저장할 기본 디렉토리. 최종 경로는 'app/interface/crawling/output/지역명' 형태가 됩니다.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="지역명 (예: 동작구). 지정하지 않으면 URL에서 자동 추출",
    )

    args = parser.parse_args()

    url = args.url
    region = args.region

    # URL 없으면 입력받기
    if not url:
        print("\n" + "=" * 80)
        print("보건소 사이트 크롤링 워크플로우")
        print("=" * 80)
        url = input("\n시작 URL을 입력하세요: ").strip()
        if not url:
            print("[ERROR] URL을 입력하지 않았습니다.")
            return

    # 지역명 결정
    region_name = region or utils.extract_region_from_url(url)
    if not region_name or region_name == "unknown":
        print(
            "경고: URL에서 지역명을 추출할 수 없거나 'unknown'입니다. 기본 디렉토리를 사용합니다."
        )
        region_name = "default_region"  # 또는 다른 기본값 사용

    # 최종 출력 디렉토리 설정
    output_dir = os.path.join(args.output_dir, region_name)

    # 워크플로우 실행
    workflow = HealthCareWorkflow(
        output_dir=output_dir, region=region_name
    )  # region_name 전달

    try:
        summary = workflow.run(start_url=url)
        print("\n[SUCCESS] 워크플로우 성공적으로 완료!")

    except Exception as e:
        print(f"\n[ERROR] 워크플로우 실패: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
