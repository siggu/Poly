"""
서울시 복지포털(wis.seoul.go.kr) 전용 크롤러

서울시 복지포털에서 건강 관련 복지 서비스 정보를 수집합니다.
- 전체 복지 서비스 목록 가져오기
- 건강 관련 키워드로 필터링
- 각 서비스 상세 정보 크롤링 및 구조화
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


class WelfareCrawler(BaseCrawler):
    """서울시 복지포털 전용 크롤러"""

    def __init__(self, output_dir: str = "app/crawling/output"):
        """
        Args:
            output_dir: 결과 저장 디렉토리
        """
        super().__init__()  # BaseCrawler 초기화
        self.output_dir = output_dir
        self.llm_crawler = LLMStructuredCrawler(model="gpt-4o-mini")
        self.link_filter = LinkFilter()  # 키워드 필터링 컴포넌트
        self.base_url = "https://wis.seoul.go.kr"
        self.search_url = "https://wis.seoul.go.kr/sec/ctg/categorySearch.do"

        os.makedirs(output_dir, exist_ok=True)

    def collect_all_services(self) -> List[Dict]:
        """
        서울시 복지포털에서 모든 복지 서비스 목록 수집

        Returns:
            복지 서비스 정보 리스트 [{'title': ..., 'description': ..., 'detail_id': ...}]
        """
        print("\n복지포털에서 전체 서비스 목록 가져오는 중...")

        try:
            # GET 방식으로 페이지 요청
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }

            response = self.session.get(self.search_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # 카드 리스트에서 항목 추출
            card_list = soup.find("ul", class_="card-ls")

            if not card_list:
                print("오류: 복지 서비스 목록을 찾을 수 없습니다.")
                return []

            items = card_list.find_all("li")
            print(f"  ✓ 총 {len(items)}개의 복지 서비스 발견")

            services = []

            for item in items:
                service_info = self._parse_service_item(item)
                if service_info:
                    services.append(service_info)

            return services

        except Exception as e:
            print(f"오류: 복지 서비스 목록 수집 실패 - {e}")
            return []

    def _parse_service_item(self, item) -> Optional[Dict]:
        """
        개별 서비스 항목 파싱

        Args:
            item: BeautifulSoup li 요소

        Returns:
            서비스 정보 딕셔너리 또는 None
        """
        try:
            title = "제목 없음"
            description = "설명 없음"
            department = ""
            phone = ""
            detail_id = ""

            # 제목과 설명 추출
            con_dl = item.find("dl", class_="con")
            if con_dl:
                title_tag = con_dl.find("dt")
                if title_tag:
                    title = title_tag.get_text(strip=True)

                desc_tag = con_dl.find("dd")
                if desc_tag:
                    description = desc_tag.get_text(strip=True)

            # 담당부서와 전화번호 추출
            cnt_div = item.find("div", class_="cnt")
            if cnt_div:
                p_tags = cnt_div.find_all("p")
                if len(p_tags) >= 2:
                    department = p_tags[0].get_text(strip=True)
                    phone = p_tags[1].get_text(strip=True)

            # 상세보기 ID 추출 (javascript:detailOpen(ID))
            detail_link = item.find("a", class_="btn-sss")
            if detail_link:
                href = detail_link.get("href", "")
                match = re.search(r"detailOpen\((\d+)\)", href)
                if match:
                    detail_id = match.group(1)

            if not detail_id:
                return None

            return {
                "title": title,
                "description": description,
                "department": department,
                "phone": phone,
                "detail_id": detail_id,
                "detail_url": f"https://wis.seoul.go.kr/sec/ctg/categoryDetail.do?id={detail_id}",
            }

        except Exception as e:
            print(f"    경고: 항목 파싱 실패 - {e}")
            return None

    def filter_health_services(self, services: List[Dict]) -> List[Dict]:
        """
        건강 관련 키워드로 서비스 필터링 (config.KEYWORD_FILTER 사용)

        Args:
            services: 전체 서비스 리스트

        Returns:
            필터링된 서비스 리스트
        """
        if config.KEYWORD_FILTER["mode"] == "none":
            return services

        print(
            f"\n[키워드 필터링] 총 {len(services)}개 서비스를 '{config.KEYWORD_FILTER['mode']}' 모드로 필터링 중..."
        )

        filtered = []
        excluded = []

        for service in services:
            combined_text = service["title"] + " " + service["description"]

            # LinkFilter의 check_keyword_filter 사용
            passed, reason = self.link_filter.check_keyword_filter(
                combined_text,
                whitelist=config.KEYWORD_FILTER.get("whitelist"),
                blacklist=config.KEYWORD_FILTER.get("blacklist"),
                mode=config.KEYWORD_FILTER["mode"],
            )

            if passed:
                filtered.append(service)
                print(f"  ✓ [포함] {service['title']}")
            else:
                excluded.append({"title": service["title"], "reason": reason})
                print(f"  ✗ [제외] {service['title']} - {reason}")

        print(
            f"\n[키워드 필터링 완료] {len(services)}개 중 {len(filtered)}개 서비스 선택됨 (제외: {len(excluded)}개)"
        )
        return filtered

    def crawl_and_structure_service(self, service_info: Dict) -> Optional[Dict]:
        """
        복지 서비스 상세 페이지 크롤링 및 구조화

        Args:
            service_info: 서비스 정보 {'title': ..., 'detail_url': ..., ...}

        Returns:
            구조화된 데이터 또는 None (실패 시)
        """
        try:
            # LLM 크롤러로 구조화
            structured_data = self.llm_crawler.crawl_and_structure(
                url=service_info["detail_url"],
                region="서울시",
                title=service_info["title"],
            )

            # 표준 필드만 반환
            return structured_data.model_dump()

        except Exception as e:
            print(f"    ✗ 크롤링 실패: {e}")
            return None

    def run_workflow(
        self,
        filter_health: bool = True,
        max_items: int = None,
        output_filename: str = None,
        return_data: bool = False,
        save_json: bool = True,
    ):
        """
        전체 워크플로우 실행: 수집 → 필터링 → 크롤링 → 저장

        Args:
            filter_health: 건강 관련만 필터링할지 여부
            max_items: 최대 처리 항목 수 (None이면 전체)
            output_filename: 출력 파일명 (None이면 자동 생성)
        """
        print("=" * 80)
        print("서울시 복지포털 크롤링 워크플로우 시작")
        print("=" * 80)

        # 1단계: 전체 서비스 목록 수집
        print("\n[1단계] 복지 서비스 목록 수집 중...")
        print("-" * 80)

        all_services = self.collect_all_services()

        if not all_services:
            print("처리할 서비스가 없습니다. 워크플로우를 종료합니다.")
            return

        # 2단계: 건강 관련 필터링 (옵션)
        if filter_health:
            print("\n[2단계] 건강 관련 서비스 필터링 중...")
            print("-" * 80)
            services_to_process = self.filter_health_services(all_services)
        else:
            services_to_process = all_services

        # 최대 개수 제한
        if max_items:
            services_to_process = services_to_process[:max_items]
            print(f"\n최대 {max_items}개로 제한하여 처리합니다.")

        # 링크 목록 저장
        links_file = os.path.join(self.output_dir, "welfare_collected_links.json")
        with open(links_file, "w", encoding="utf-8") as f:
            json.dump(services_to_process, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 처리 대상: {len(services_to_process)}개")
        print(f"✓ 링크 목록 저장: {links_file}")

        # 3단계: 각 서비스 크롤링 및 구조화
        print("\n[3단계] 서비스 크롤링 및 구조화 중...")
        print("-" * 80)

        all_results = []
        success_count = 0
        fail_count = 0

        for idx, service_info in enumerate(services_to_process, 1):
            print(f"\n진행: {idx}/{len(services_to_process)} - {service_info['title']}")

            result = self.crawl_and_structure_service(service_info)

            if result:
                all_results.append(result)
                success_count += 1
                print("  ✓ 완료")
            else:
                fail_count += 1

            # API 제한 고려하여 약간의 지연
            if idx < len(services_to_process):
                time.sleep(1)

        # 4단계: 결과 저장
        print("\n[4단계] 결과 저장 중...")
        print("-" * 80)

        output_path = None
        if save_json:
            if output_filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"welfare_structured_data_{timestamp}.json"
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)

        # 결과 요약
        print("\n" + "=" * 80)
        print("워크플로우 완료")
        print("=" * 80)
        print(f"✓ 전체 서비스: {len(all_services)}개")
        if filter_health:
            print(f"✓ 필터링된 서비스: {len(services_to_process)}개")
        print(f"✓ 성공: {success_count}개")
        print(f"✗ 실패: {fail_count}개")
        if save_json:
            print(f"✓ 결과 파일: {output_path}")
        print("=" * 80)

        if return_data:
            return all_results


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description="서울시 복지포털 전용 크롤러")
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="건강 관련 필터링 비활성화 (전체 서비스 크롤링)",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        help="최대 처리 항목 수 (기본값: 전체)",
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
    crawler = WelfareCrawler(output_dir=args.output_dir)

    try:
        crawler.run_workflow(
            filter_health=not args.no_filter,
            max_items=args.max_items,
            output_filename=args.output,
        )
    except Exception as e:
        print(f"\n✗ 워크플로우 실패: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
