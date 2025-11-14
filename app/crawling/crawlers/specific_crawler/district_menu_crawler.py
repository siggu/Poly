"""
통합 메뉴 크롤러 - 6개 구 대응
은평구, 강동구, 종로구, 중랑구, 영등포구, 용산구

각 구별 Strategy 패턴을 사용하여 메뉴 수집 방식을 동적으로 선택
"""

from ..district_crawler import DistrictCrawler
from typing import List, Dict
from .district_configs import DISTRICT_CONFIGS


class DistrictMenuCrawler(DistrictCrawler):
    """통합 메뉴 크롤러 - Strategy 패턴 사용"""

    def __init__(
        self,
        district_name: str,
        start_url: str = None,
        output_dir: str = None,
        max_workers: int = 4,
    ):
        """
        Args:
            district_name: 구 이름 (예: "은평구", "강동구")
            start_url: 시작 URL (None이면 config에서 가져옴)
            output_dir: 출력 디렉토리 (None이면 config에서 가져옴)
            max_workers: 병렬 처리 worker 수
        """
        # district config 가져오기
        if district_name not in DISTRICT_CONFIGS:
            raise ValueError(
                f"지원하지 않는 구: {district_name}. "
                f"지원 구: {list(DISTRICT_CONFIGS.keys())}"
            )

        config = DISTRICT_CONFIGS[district_name]

        # 기본값 설정
        self.district_name = district_name
        self.start_url = start_url or config["start_url"]
        final_output_dir = output_dir or config["output_dir"]

        # 부모 초기화
        super().__init__(
            output_dir=final_output_dir, region=district_name, max_workers=max_workers
        )

        # Strategy 로드
        self._load_strategy(config["strategy"])

    def _load_strategy(self, strategy_name: str):
        """
        동적으로 strategy 모듈을 로드하여 collect_menu_links 함수 가져오기

        Args:
            strategy_name: strategy 모듈명 (예: "ep_strategy")
        """
        try:
            # 동적 import
            strategy_module = __import__(
                f"app.crawling.crawlers.specific_crawler.strategies.{strategy_name}",
                fromlist=["collect_menu_links"],
            )
            self.collect_menu_links_func = strategy_module.collect_menu_links
            print(f"[{self.district_name}] Strategy 로드 완료: {strategy_name}")

        except ImportError as e:
            raise ImportError(
                f"Strategy 모듈을 찾을 수 없습니다: {strategy_name}. 오류: {e}"
            )
        except AttributeError:
            raise AttributeError(
                f"Strategy 모듈 '{strategy_name}'에 collect_menu_links 함수가 없습니다."
            )

    def collect_initial_items(
        self,
        *,
        start_url: str,
        crawl_rules: List[Dict],
        enable_keyword_filter: bool,
        **kwargs,
    ) -> List[Dict]:
        """
        초기 링크 수집 (구별 strategy 사용)

        Returns:
            수집된 링크 목록
        """
        print(f"\n[1단계] {self.district_name} 메뉴 링크 수집 중...")
        print(f"  시작 URL: {start_url}")
        print("-" * 80)

        # 페이지 가져오기
        soup = self.fetch_page(start_url)
        if not soup:
            print(f"오류: 시작 URL({start_url})에 접근할 수 없습니다.")
            return []

        # Strategy 패턴: 각 구별 메뉴 수집 로직 실행
        initial_links = self.collect_menu_links_func(soup, start_url)

        print(f"\n[SUCCESS] 총 {len(initial_links)}개의 초기 링크 수집 완료")

        # 키워드 필터링
        if enable_keyword_filter:
            from ... import config

            if config.KEYWORD_FILTER["mode"] != "none":
                print("\n[1.2단계] 키워드 기반 링크 필터링...")
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


if __name__ == "__main__":
    import sys

    # 테스트: 구 이름을 인자로 받아서 실행
    if len(sys.argv) > 1:
        district = sys.argv[1]
    else:
        district = "은평구"  # 기본값

    print(f"\n{'=' * 80}")
    print(f"{district} 메뉴 크롤러 테스트")
    print(f"{'=' * 80}\n")

    crawler = DistrictMenuCrawler(district_name=district)
    crawler.run(start_url=crawler.start_url)
