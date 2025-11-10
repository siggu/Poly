"""
e보건소(www.e-health.go.kr) 전용 크롤러

e보건소는 게시판 구조로 되어 있어 일반 보건소 사이트와 다른 처리가 필요합니다.
- 카테고리별 게시판 목록 페이지
- 페이징 처리 (페이지당 10개 항목)
- 게시글 상세 페이지 크롤링
"""

from bs4 import BeautifulSoup
import json
import re
from typing import List, Dict, Optional
import os
import sys
from datetime import datetime
import time

# 공통 모듈 import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from base.base_crawler import BaseCrawler
from base.llm_crawler import LLMStructuredCrawler
from components.link_filter import LinkFilter


class EHealthCrawler(BaseCrawler):
    """e보건소 전용 크롤러"""

    def __init__(self, output_dir: str = "app/crawling/output"):
        """
        Args:
            output_dir: 결과 저장 디렉토리
        """
        super().__init__()  # BaseCrawler 초기화
        self.output_dir = output_dir
        self.llm_crawler = LLMStructuredCrawler(model="gpt-4o-mini")
        self.link_filter = LinkFilter()  # 키워드 필터링 컴포넌트

        os.makedirs(output_dir, exist_ok=True)

    def get_list_page_url(self, category_name: str, page_index: int = 1) -> str:
        """
        카테고리의 목록 페이지 URL 생성

        Args:
            category_name: 카테고리 이름 (예: "건강증진")
            page_index: 페이지 번호 (1부터 시작)

        Returns:
            목록 페이지 URL
        """
        category_info = config.EHEALTH_CATEGORIES.get(category_name)
        if not category_info:
            raise ValueError(f"알 수 없는 카테고리: {category_name}")

        url = (
            f"{config.EHEALTH_BASE_URL}/gh/heSrvc/selectBbsDtlInfo.do"
            f"?bbsId={config.EHEALTH_BBS_ID}"
            f"&bbsSeCd={category_info['bbsSeCd']}"
            f"&menuId={category_info['menuId']}"
            f"&pageIndex={page_index}"
        )
        return url

    def get_detail_page_url(self, bbs_no: str, menu_id: str) -> str:
        """
        게시글 상세 페이지 URL 생성

        Args:
            bbs_no: 게시글 번호
            menu_id: 메뉴 ID

        Returns:
            상세 페이지 URL
        """
        url = (
            f"{config.EHEALTH_BASE_URL}/gh/heSrvc/selectBbsDtlViewInfo.do"
            f"?bbsId={config.EHEALTH_BBS_ID}"
            f"&bbsNo={bbs_no}"
            f"&menuId={menu_id}"
        )
        return url

    def parse_total_count(self, soup: BeautifulSoup) -> int:
        """
        목록 페이지에서 전체 건수 파싱

        Args:
            soup: BeautifulSoup 객체

        Returns:
            전체 건수
        """
        # <div class="page_num">전체건수 <em>15</em>건</div>
        page_num_div = soup.select_one(".page_num em")
        if page_num_div:
            try:
                return int(page_num_div.get_text(strip=True))
            except ValueError:
                pass
        return 0

    def extract_article_numbers(self, soup: BeautifulSoup) -> List[str]:
        """
        목록 페이지에서 게시글 번호 추출

        Args:
            soup: BeautifulSoup 객체

        Returns:
            게시글 번호 리스트
        """
        article_numbers = []

        # <a href="#" onclick="fn_moveDetail('418333'); return false;">
        list_items = soup.select(".list_wrap li a")

        for item in list_items:
            onclick = item.get("onclick", "")
            # fn_moveDetail('418333') 형식에서 번호 추출
            match = re.search(r"fn_moveDetail\('(\d+)'\)", onclick)
            if match:
                article_numbers.append(match.group(1))

        return article_numbers

    def collect_category_links(
        self, category_name: str, max_pages: int = None
    ) -> List[Dict]:
        """
        특정 카테고리의 모든 게시글 링크 수집

        Args:
            category_name: 카테고리 이름
            max_pages: 최대 페이지 수 (None이면 전체)

        Returns:
            게시글 정보 리스트 [{'name': '...', 'url': '...', 'bbs_no': '...'}]
        """
        category_info = config.EHEALTH_CATEGORIES.get(category_name)
        if not category_info:
            raise ValueError(f"알 수 없는 카테고리: {category_name}")

        print(f"\n[{category_name}] 카테고리 크롤링 시작...")

        # 첫 페이지로 전체 건수 확인
        first_page_url = self.get_list_page_url(category_name, 1)
        response = self.session.get(first_page_url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        total_count = self.parse_total_count(soup)
        total_pages = (total_count + 9) // 10  # 페이지당 10개, 올림 처리

        if max_pages:
            total_pages = min(total_pages, max_pages)

        print(f"  전체 {total_count}건, {total_pages}페이지")

        all_articles = []

        # 각 페이지 순회
        for page in range(1, total_pages + 1):
            print(f"  페이지 {page}/{total_pages} 처리 중...")

            if page > 1:
                page_url = self.get_list_page_url(category_name, page)
                response = self.session.get(page_url, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")

            # 게시글 번호 추출
            article_numbers = self.extract_article_numbers(soup)

            # 게시글 제목도 함께 추출
            list_items = soup.select(".list_wrap li")
            for idx, item in enumerate(list_items):
                if idx < len(article_numbers):
                    title_elem = item.select_one(".list_top")
                    title = (
                        title_elem.get_text(strip=True) if title_elem else "제목 없음"
                    )

                    bbs_no = article_numbers[idx]
                    detail_url = self.get_detail_page_url(
                        bbs_no, category_info["menuId"]
                    )

                    all_articles.append(
                        {
                            "name": title,
                            "url": detail_url,
                            "bbs_no": bbs_no,
                            "category": category_name,
                        }
                    )

            # 너무 빠르게 요청하지 않도록 약간의 지연
            if page < total_pages:
                time.sleep(0.5)

        print(f"  ✓ 총 {len(all_articles)}개 게시글 발견")
        return all_articles

    def collect_all_links(
        self, categories: List[str] = None, max_pages_per_category: int = None
    ) -> List[Dict]:
        """
        모든 카테고리 또는 지정된 카테고리의 게시글 링크 수집

        Args:
            categories: 수집할 카테고리 리스트 (None이면 전체)
            max_pages_per_category: 카테고리당 최대 페이지 수

        Returns:
            모든 게시글 정보 리스트
        """
        if categories is None:
            categories = list(config.EHEALTH_CATEGORIES.keys())

        all_links = []
        for category in categories:
            try:
                links = self.collect_category_links(
                    category, max_pages=max_pages_per_category
                )
                all_links.extend(links)
            except Exception as e:
                print(f"  ✗ {category} 카테고리 처리 실패: {e}")

        return all_links

    def _filter_links_by_keywords(self, links: List[Dict]) -> List[Dict]:
        """
        키워드 기반 게시글 제목 필터링

        Args:
            links: 게시글 정보 리스트 [{"name": str, ...}, ...]

        Returns:
            필터링된 링크 목록
        """
        # district_crawler의 filter_by_keywords와 동일한 로직 사용
        # links 형식을 link_filter가 받을 수 있도록 변환
        links_to_filter = [{"name": link["name"], "url": link["url"]} for link in links]

        # LinkFilter로 필터링
        filtered_simple = self.link_filter.filter_by_keywords(
            links_to_filter,
            whitelist=config.KEYWORD_FILTER.get("whitelist"),
            blacklist=config.KEYWORD_FILTER.get("blacklist"),
            mode=config.KEYWORD_FILTER["mode"],
        )

        # 필터링된 URL 집합 생성
        filtered_urls = {link["url"] for link in filtered_simple}

        # 원본 links에서 필터링된 것만 반환 (전체 정보 유지)
        filtered_links = [link for link in links if link["url"] in filtered_urls]

        return filtered_links

    def crawl_and_structure_article(self, article_info: Dict) -> Optional[Dict]:
        """
        게시글 상세 페이지 크롤링 및 구조화

        Args:
            article_info: 게시글 정보 {'name': ..., 'url': ..., ...}

        Returns:
            구조화된 데이터 또는 None (실패 시)
        """
        try:
            structured_data = self.llm_crawler.crawl_and_structure(
                url=article_info["url"], region="전국"
            )

            # 표준 필드만 반환: id, title, support_target, support_content, raw_text, source_url, region
            return structured_data.model_dump()
        except Exception as e:
            print(f"    ✗ 크롤링 실패: {e}")
            return None

    def run_workflow(
        self,
        categories: List[str] = None,
        max_pages_per_category: int = None,
        output_filename: str = None,
        return_data: bool = False,
        save_json: bool = True,
    ):
        """
        전체 워크플로우 실행: 링크 수집 → 크롤링 → 저장

        Args:
            categories: 수집할 카테고리 리스트 (None이면 전체)
            max_pages_per_category: 카테고리당 최대 페이지 수
            output_filename: 출력 파일명 (None이면 자동 생성)
        """
        print("=" * 80)
        print("e보건소 크롤링 워크플로우 시작")
        print("=" * 80)

        # 1단계: 링크 수집
        print("\n[1단계] 게시글 링크 수집 중...")
        print("-" * 80)

        links = self.collect_all_links(
            categories=categories, max_pages_per_category=max_pages_per_category
        )

        # 링크 목록 저장
        links_file = os.path.join(self.output_dir, "ehealth_collected_links.json")
        with open(links_file, "w", encoding="utf-8") as f:
            json.dump(links, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 총 {len(links)}개 링크 수집 완료")
        print(f"✓ 링크 목록 저장: {links_file}")

        # 1.5단계: 키워드 필터링 (config.KEYWORD_FILTER 사용)
        if config.KEYWORD_FILTER["mode"] != "none":
            print("\n[1.5단계] 키워드 기반 링크 필터링...")
            print("-" * 80)
            links = self._filter_links_by_keywords(links)

            if not links:
                print(
                    "키워드 필터링 후 처리할 링크가 없습니다. 워크플로우를 종료합니다."
                )
                return

        # 2단계: 각 게시글 크롤링 및 구조화
        print("\n[2단계] 게시글 크롤링 및 구조화 중...")
        print("-" * 80)

        all_results = []
        success_count = 0
        fail_count = 0

        for idx, article_info in enumerate(links, 1):
            print(
                f"\n진행: {idx}/{len(links)} - {article_info['category']}: {article_info['name']}"
            )

            result = self.crawl_and_structure_article(article_info)

            if result:
                all_results.append(result)
                success_count += 1
                print("  ✓ 완료")
            else:
                fail_count += 1

            # API 제한 고려하여 약간의 지연
            if idx < len(links):
                time.sleep(1)

        # 3단계: 결과 저장/반환
        print("\n[3단계] 결과 저장/반환 중...")
        print("-" * 80)
        output_path = None
        if save_json:
            if output_filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"ehealth_structured_data_{timestamp}.json"
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)

        # 결과 요약
        print("\n" + "=" * 80)
        print("워크플로우 완료")
        print("=" * 80)
        print(f"✓ 전체 링크: {len(links)}개")
        print(f"✓ 성공: {success_count}개")
        print(f"✗ 실패: {fail_count}개")
        if save_json:
            print(f"✓ 결과 파일: {output_path}")
        if return_data:
            return all_results
        print("=" * 80)


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description="e보건소 전용 크롤러")
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=list(config.EHEALTH_CATEGORIES.keys()),
        help="수집할 카테고리 (기본값: 전체)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="카테고리당 최대 페이지 수 (기본값: 전체)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="출력 파일명 (기본값: 자동 생성)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="app/crawling/output",
        help="출력 디렉토리 (기본값: app/crawling/output)",
    )

    args = parser.parse_args()

    # 크롤러 생성 및 실행
    crawler = EHealthCrawler(output_dir=args.output_dir)

    try:
        crawler.run_workflow(
            categories=args.categories,
            max_pages_per_category=args.max_pages,
            output_filename=args.output,
        )
    except Exception as e:
        print(f"\n✗ 워크플로우 실패: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
