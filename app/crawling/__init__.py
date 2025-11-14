"""
크롤링 모듈

구조:
- config.py: 크롤링 규칙 및 설정
- utils.py: 공통 유틸리티 함수
- base/: 베이스 클래스들
  - base_crawler.py: HTTP 크롤링 베이스
  - llm_crawler.py: LLM 구조화 베이스
  - parallel_crawler.py: 병렬 크롤링 베이스
  - workflow_crawler.py: 워크플로우 크롤러 베이스
- components/: 재사용 가능한 컴포넌트
  - link_collector.py: 링크 수집
  - link_filter.py: 링크 필터링
  - page_processor.py: 페이지 처리
- crawlers/: 크롤러 구현체들
  - district_crawler.py: 구 보건소 크롤러 (기본)
  - run_crawler.py: 크롤러 실행기
  - specific_crawler/: 특정 구 전용 크롤러
    - district_menu_crawler.py: 통합 메뉴 크롤러 (Strategy Pattern)
    - district_configs.py: 구별 설정
    - strategies/: 구별 메뉴 수집 전략
      - base_strategy.py: 전략 베이스 클래스
      - ep_strategy.py: 은평구
      - gangdong_strategy.py: 강동구
      - gwanak_strategy.py: 관악구
      - ddm_strategy.py: 동대문구
      - jongno_strategy.py: 종로구
      - jungnang_strategy.py: 중랑구
      - junggu_strategy.py: 중구
      - sd_strategy.py: 성동구
      - ydp_strategy.py: 영등포구
      - yongsan_strategy.py: 용산구
    - songpa_crawler.py: 송파구 전용 크롤러
    - yangcheon_crawler.py: 양천구 전용 크롤러
    - mapo_crawler.py: 마포구 전용 크롤러
    - welfare_crawler.py: 서울시 복지포털 크롤러
    - ehealth_crawler.py: e보건소 크롤러
- crawler_factory.py: URL 기반 크롤러 자동 선택
"""

from .base import BaseCrawler, LLMStructuredCrawler, HealthSupportInfo
from .crawlers import (
    DistrictCrawler,
    EHealthCrawler,
    WelfareCrawler,
    SongpaCrawler,
    YangcheonCrawler,
    MapoCrawler,
    DistrictMenuCrawler,
)
from .crawlers.specific_crawler import district_configs

__all__ = [
    "BaseCrawler",
    "LLMStructuredCrawler",
    "HealthSupportInfo",
    "DistrictCrawler",
    "DistrictMenuCrawler",
    "EHealthCrawler",
    "WelfareCrawler",
    "SongpaCrawler",
    "YangcheonCrawler",
    "MapoCrawler",
    "district_configs",
]
