"""
워크플로우: 링크 수집 -> 크롤링 및 구조화 (탭 처리 포함 - 컨테이너 페이지 저장)

1. 초기 링크 수집: 보건소 사이트의 LNB 등에서 서브 메뉴 링크 수집
2. 링크 처리 루프:
   - 각 링크 페이지 방문
   - 페이지 내부에 탭 메뉴가 있는지 확인
   - ★★★ 현재 페이지 내용을 LLM으로 구조화 (탭 유무와 상관없이 항상 실행) ★★★
   - 탭 발견 시: 새로운 탭 링크들을 처리 목록에 추가
3. 모든 결과를 JSON 파일로 저장
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Sequence, Set, Tuple


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from app.crawling import config, utils
from app.crawling.utils import normalize_url
from app.crawling.base.workflow_crawler import WorkflowCrawler
from app.crawling.components.link_collector import LinkCollector


class DistrictCrawler(WorkflowCrawler):
    """보건소 사이트 크롤링 및 구조화 워크플로우"""

    def __init__(
        self,
        output_dir: str = "app/crawling/output",
        region: str = None,
        max_workers: int = 4,
    ):
        """
        Args:
            output_dir: 결과 저장 디렉토리
            region: 지역명 (예: "동작구"). None이면 URL에서 자동 추출 시도
            max_workers: 병렬 처리 워커 수 (기본값: 4)
        """
        super().__init__(output_dir=output_dir, max_workers=max_workers)
        self.region = region

        # DistrictCrawler 전용 컴포넌트
        self.link_collector = LinkCollector()

        # 탭 제외 로그 중복 방지용 (URL 기준으로 추적)
        self.excluded_tab_urls = set()

    def run(
        self,
        start_url: str,
        crawl_rules: List[Dict] = None,
        save_links: bool = True,
        save_json: bool = True,
        return_data: bool = False,
        enable_keyword_filter: bool = True,
    ) -> Dict:
        """
        전체 워크플로우 실행

        Args:
            start_url: 시작 URL
            crawl_rules: 크롤링 규칙 (None이면 config에서 가져옴)
            save_links: 초기 링크 저장 여부
            enable_keyword_filter: 키워드 필터링 활성화 여부

        Returns:
            워크플로우 요약 정보
        """
        print("=" * 80)
        print("보건소 사이트 크롤링 워크플로우 시작")
        print("=" * 80)

        crawl_rules = crawl_rules or config.CRAWL_RULES

        summary = super().run(
            start_url=start_url,
            save_initial=save_links,
            save_json=save_json,
            return_data=return_data,
            enable_keyword_filter=enable_keyword_filter,
            crawl_rules=crawl_rules,
        )

        return summary

    def collect_initial_items(
        self,
        *,
        start_url: str,
        crawl_rules: List[Dict],
        enable_keyword_filter: bool,
        **__: Any,
    ) -> List[Dict]:
        """
        링크 수집 및 필터링

        Returns:
            필터링된 링크 목록
        """
        print("\n[1단계] 초기 링크 수집 중...")
        print("-" * 80)

        rules_to_use = crawl_rules or config.CRAWL_RULES
        initial_links = self.link_collector.collect_links(start_url, rules_to_use)
        print(f"\n[SUCCESS] 총 {len(initial_links)}개의 초기 링크 수집 완료")

        if not initial_links:
            return []

        # 키워드 필터링
        if enable_keyword_filter and config.KEYWORD_FILTER["mode"] != "none":
            print("\n[1.3단계] 키워드 기반 링크 필터링...")
            print("-" * 80)

            initial_links = self.link_filter.filter_by_keywords(
                initial_links,
                whitelist=config.KEYWORD_FILTER.get("whitelist"),
                blacklist=config.KEYWORD_FILTER.get("blacklist"),
                mode=config.KEYWORD_FILTER["mode"],
            )

            if not initial_links:
                print("키워드 필터링 후 처리할 링크가 없습니다.")

        return initial_links

    def save_initial_items(
        self,
        *,
        start_url: str,
        items: Sequence[Dict],
        **kwargs: Any,
    ) -> None:
        """초기 링크 JSON 저장"""
        links_file = os.path.join(self.output_dir, "collected_initial_links.json")
        try:
            with open(links_file, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"[FILE] 초기 링크 목록 저장: {links_file}")
        except IOError as e:
            print(f"경고: 초기 링크 파일 저장 실패 - {e}")

    def process_items_for_workflow(
        self,
        *,
        initial_items: Sequence[Dict],
        enable_keyword_filter: bool,
        **kwargs: Any,
    ) -> Tuple[List[Dict], List[Dict], int]:
        """
        모든 페이지 병렬 처리 (탭 포함)

        Returns:
            (structured_data_list, failed_urls, processed_count) 튜플
        """
        print("\n[2단계] 페이지 처리 및 LLM 구조화 (병렬 처리, 탭 포함)...")
        print(f"  - 병렬 워커 수: {self.max_workers}")
        print("-" * 80)

        structured_data_list = []
        failed_urls = []
        links_to_process = list(initial_items)
        # URL을 정규화하여 저장 (중복 방지)

        processed_or_queued_urls = {
            normalize_url(link["url"]) for link in initial_items
        }
        processed_count = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Future 객체를 저장할 딕셔너리
            future_to_link = {}

            # 초기 링크들을 제출
            for link_info in links_to_process[: self.max_workers]:
                future = executor.submit(
                    self._process_single_page_wrapper,
                    link_info,
                    processed_count + 1,
                    len(processed_or_queued_urls),
                )
                future_to_link[future] = link_info
                processed_count += 1

            # 이미 제출된 링크는 큐에서 제거
            links_to_process = links_to_process[self.max_workers :]

            # Future 완료될 때마다 처리
            while future_to_link:
                for future in as_completed(future_to_link):
                    link_info = future_to_link.pop(future)

                    try:
                        success, structured_data, tab_links, final_url = future.result()

                        if success:
                            with self.lock:
                                structured_data_list.append(
                                    structured_data.model_dump()
                                )

                                # 최종 URL을 processed_or_queued_urls에 추가 (리다이렉트 추적)
                                if final_url:
                                    normalized_final = normalize_url(final_url)
                                    if normalized_final not in processed_or_queued_urls:
                                        processed_or_queued_urls.add(normalized_final)

                            # 탭 링크 처리
                            if tab_links:
                                with self.lock:
                                    newly_added = self._add_tab_links_to_queue(
                                        tab_links,
                                        links_to_process,
                                        processed_or_queued_urls,
                                        enable_keyword_filter,
                                    )
                        else:
                            # 실패 기록
                            with self.lock:
                                failed_urls.append(structured_data)

                    except Exception as e:
                        print(f"  [ERROR] Future 처리 중 오류: {e}")
                        import traceback

                        traceback.print_exc()

                    # 큐에 남은 링크가 있으면 새 작업 제출
                    if links_to_process:
                        next_link = links_to_process.pop(0)
                        processed_count += 1
                        new_future = executor.submit(
                            self._process_single_page_wrapper,
                            next_link,
                            processed_count,
                            len(processed_or_queued_urls),
                        )
                        future_to_link[new_future] = next_link

        return structured_data_list, failed_urls, processed_count

    def _process_single_page_wrapper(
        self, link_info: Dict, processed_count: int, total_estimate: int
    ) -> tuple:
        """
        _process_single_page의 thread-safe 래퍼
        병렬 처리 시 출력이 섞이지 않도록 로그를 버퍼에 모았다가 한 번에 출력
        """
        # 로그 버퍼 생성
        log_buffer = []

        url = link_info["url"]
        name = link_info["name"]

        # 시작 로그
        log_buffer.append(f"\n[{processed_count}/{total_estimate}*] 처리 시도: {name}")
        log_buffer.append(f"  URL: {url}")

        # 실제 처리 (로그 버퍼 전달)
        success, result, tab_links, final_url = self._process_single_page(
            link_info, processed_count, total_estimate, log_buffer
        )

        # 완료 후 한 번에 출력 (lock 사용)
        with self.lock:
            for log_line in log_buffer:
                print(log_line)

        return success, result, tab_links, final_url

    def _process_single_page(
        self,
        link_info: Dict,
        processed_count: int,
        total_estimate: int,
        log_buffer: list,
    ) -> tuple:
        """
        단일 페이지 처리 (실제 작업 수행)

        Args:
            link_info: 링크 정보
            processed_count: 처리 순번
            total_estimate: 전체 추정 개수
            log_buffer: 로그 메시지를 저장할 리스트

        Returns:
            (success, structured_data_or_error, tab_links) 튜플
        """
        url = link_info["url"]
        name = link_info["name"]

        # time.sleep 시간 측정 (개선 포인트 확인용)
        sleep_start = time.time()
        time.sleep(0.2)
        sleep_duration = time.time() - sleep_start
        utils.get_timing_stats().add_timing("4_Sleep대기", sleep_duration)

        page_start_time = time.time()

        try:
            # 1. 페이지 가져오기 (최종 URL도 함께 받음)
            soup, final_url = self.fetch_page(url, return_final_url=True)
            if not soup:
                raise ValueError("페이지 내용을 가져올 수 없습니다.")

            # 리다이렉트 감지 및 로깅
            if final_url and final_url != url:
                if normalize_url(final_url) != normalize_url(url):
                    log_buffer.append(f"    !리다이렉트 감지: {url} → {final_url}")
                    # 최종 URL을 link_info에 저장 (나중에 참조 가능)
                    link_info["final_url"] = final_url

            # 2. 탭 메뉴 확인
            tab_links = self.page_processor.find_tabs_on_page(soup, final_url or url)

            # 3. 제목 결정
            title_for_llm = self.page_processor.determine_page_title(
                name, url, tab_links
            )

            # 3-1. 탭 제목이 블랙리스트에 해당하는지 체크
            if title_for_llm != name and config.KEYWORD_FILTER["mode"] != "none":
                # 탭으로 제목이 변경된 경우, 블랙리스트 체크
                passed, reason = self.link_filter.check_keyword_filter(
                    title_for_llm,
                    whitelist=config.KEYWORD_FILTER.get("whitelist"),
                    blacklist=config.KEYWORD_FILTER.get("blacklist"),
                    mode=config.KEYWORD_FILTER["mode"],
                )
                if not passed:
                    log_buffer.append(f"  [SKIP] 탭 제목 필터링: {reason}")
                    # 실패로 처리하지 않고 건너뜀
                    raise ValueError(
                        f"탭 제목이 블랙리스트에 해당: {title_for_llm} - {reason}"
                    )

            # 4. LLM 구조화
            log_buffer.append("    -> 내용 구조화 진행...")

            region = self.region or utils.extract_region_from_url(url)
            structured_data = self.llm_crawler.crawl_and_structure(
                url=url, region=region, title=title_for_llm
            )

            page_duration = time.time() - page_start_time
            utils.get_timing_stats().add_timing("5_페이지처리_전체", page_duration)

            log_buffer.append(f"  [SUCCESS] 성공 (소요: {page_duration:.2f}초)")

            # 최종 URL을 반환 (리다이렉트 추적용)
            return True, structured_data, tab_links, final_url

        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            error_info = {
                "url": url,
                "name": name,
                "error": str(e),
                "details": error_details,
            }

            log_buffer.append(f"  [ERROR] 실패: {e}")
            log_buffer.append(f"  오류 상세:\n{error_details}")

            return False, error_info, [], None

    def _add_tab_links_to_queue(
        self,
        tab_links: List[Dict],
        links_to_process: List[Dict],
        processed_or_queued_urls: Set[str],
        enable_keyword_filter: bool,
    ) -> int:
        """
        탭 링크를 처리 큐에 추가 (중복 및 키워드 필터링 체크)

        Returns:
            추가된 탭 링크 수
        """
        newly_added_count = 0
        excluded_count = 0

        for tab_link_info in tab_links:
            tab_url = tab_link_info["url"]
            tab_name = tab_link_info["name"]

            # 중복 체크 (정규화된 URL 기준)
            normalized_tab_url = normalize_url(tab_url)
            if normalized_tab_url in processed_or_queued_urls:
                continue

            # 키워드 필터링 체크
            if enable_keyword_filter and config.KEYWORD_FILTER["mode"] != "none":
                passed, reason = self.link_filter.check_keyword_filter(
                    tab_name,
                    whitelist=config.KEYWORD_FILTER.get("whitelist"),
                    blacklist=config.KEYWORD_FILTER.get("blacklist"),
                    mode=config.KEYWORD_FILTER["mode"],
                )

                if not passed:
                    # 같은 URL의 제외 로그는 한 번만 출력 (중복 방지)
                    if tab_url not in self.excluded_tab_urls:
                        self.excluded_tab_urls.add(tab_url)
                        print(
                            f"      ✗ [같은 화면의 다른 탭 제외] {tab_name} - {reason}"
                        )
                    excluded_count += 1
                    continue

            # 큐에 추가 (정규화된 URL을 processed_or_queued_urls에 저장)
            links_to_process.append(tab_link_info)
            processed_or_queued_urls.add(normalized_tab_url)
            newly_added_count += 1
            print(f"      + 탭 링크 추가: {tab_name} ({tab_url})")

        if newly_added_count > 0:
            print(
                f"    -> 새로운 탭 링크 {newly_added_count}개를 처리 목록에 추가했습니다."
            )

        return newly_added_count

    def persist_results(
        self,
        *,
        start_url: str,
        initial_items: Sequence[Dict],
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        processed_count: int,
        save_json: bool = True,
        return_data: bool = False,
        **kwargs: Any,
    ) -> Dict:
        """
        결과 저장

        Returns:
            요약 정보 딕셔너리
        """
        print("\n[3단계] 결과 저장 중...")
        print("-" * 80)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        region_name = self.region or utils.extract_region_from_url(start_url)

        output_file = None
        if save_json:
            # 전체 구조화 데이터 저장
            output_file = os.path.join(
                self.output_dir, f"structured_data_{region_name}.json"
            )
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(structured_items, f, ensure_ascii=False, indent=2)
                print(f"[SUCCESS] 구조화 데이터 저장: {output_file}")
            except IOError as e:
                print(f"오류: 구조화 데이터 파일 저장 실패 - {e}")

        # 실패한 URL 저장
        failed_file = None
        if failed_items:
            failed_file = os.path.join(
                self.output_dir, f"failed_urls_{region_name}.json"
            )
            try:
                with open(failed_file, "w", encoding="utf-8") as f:
                    json.dump(failed_items, f, ensure_ascii=False, indent=2)
                print(f"[WARNING] 실패한 URL 저장: {failed_file}")
            except IOError as e:
                print(f"경고: 실패한 URL 파일 저장 실패 - {e}")

        # 요약 정보
        summary = {
            "timestamp": timestamp,
            "region": region_name,
            "start_url": start_url,
            "initial_links_collected": len(initial_items),
            "total_urls_processed_or_failed": processed_count,
            "successful_structured": len(structured_items),
            "failed_processing": len(failed_items),
            "output_file": output_file,
            "failed_urls_file": failed_file,
        }
        if return_data:
            summary["data"] = structured_items  # 메모리 데이터 동봉
        summary_file = os.path.join(self.output_dir, f"summary_{timestamp}.json")
        try:
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            print(f"[FILE] 요약 정보 저장: {summary_file}")
        except IOError as e:
            print(f"경고: 요약 파일 저장 실패 - {e}")

        return summary

    def print_workflow_summary(
        self,
        *,
        initial_items: Sequence[Dict],
        processed_count: int,
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        **kwargs: Any,
    ) -> None:
        """최종 요약 출력"""
        print("\n" + "=" * 80)
        print("워크플로우 완료")
        print("=" * 80)
        print(f"[STAT] 초기 수집 링크 수: {len(initial_items)}")
        print(f"[STAT] 총 처리 시도 URL 수: {processed_count}")
        print(f"[SUCCESS] 성공 (구조화): {len(structured_items)}개")
        print(f"[ERROR] 실패: {len(failed_items)}개")
        print(f"[DIR] 결과 저장 위치: {self.output_dir}")
        print("=" * 80)

    def on_workflow_complete(
        self,
        *,
        initial_items: Sequence[Dict],
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        summary: Dict,
        **kwargs: Any,
    ) -> None:
        print("\n")
        utils.get_timing_stats().print_summary()


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(
        description="보건소 사이트 크롤링 및 구조화 워크플로우"
    )
    parser.add_argument("--url", type=str, help="시작 URL (보건소 보건사업 페이지)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="app/crawling/output",
        help="결과를 저장할 기본 디렉토리. 최종 경로는 'app/crawling/output/지역명' 형태가 됩니다.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="지역명 (예: 동작구). 지정하지 않으면 URL에서 자동 추출",
    )
    parser.add_argument(
        "--no-keyword-filter",
        action="store_true",
        help="키워드 기반 링크 필터링 비활성화",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="병렬 처리 워커 수 (기본값: 4, 권장: 2~8)",
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
        region_name = "default_region"

    # 최종 출력 디렉토리 설정
    output_dir = os.path.join(args.output_dir, region_name)

    # 워크플로우 실행
    workflow = DistrictCrawler(
        output_dir=output_dir, region=region_name, max_workers=args.max_workers
    )

    try:
        summary = workflow.run(
            start_url=url,
            enable_keyword_filter=not args.no_keyword_filter,
        )
        print("\n[SUCCESS] 워크플로우 성공적으로 완료!")

    except Exception as e:
        print(f"\n[ERROR] 워크플로우 실패: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
