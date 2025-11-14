"""
크롤링 모듈

구조:
- config.py: 크롤링 규칙 및 설정
- utils.py: 공통 유틸리티 함수
- base/: 베이스 클래스들
  - base_crawler.py: HTTP 크롤링 베이스
  - llm_crawler.py: LLM 구조화 베이스
  - parallel_crawler.py: 병렬 크롤링 베이스
- crawlers/: 크롤러 구현체들
  - district_crawler.py: 구 보건소 크롤러
  - run_crawler.py: 크롤러 실행기
  specific_crawler/: 특정 구 전용 크롤러
    - songpa_crawler.py: 송파구 크롤러
    - yangcheon_crawler.py: 양천구 크롤러
    - ydp_crawler.py: 영등포구 크롤러
    - yongsan_crawler.py: 용산구 크롤러
    - ep_crawler.py: 은평구 크롤러
    - jongno_crawler.py: 종로구 크롤러
    - jungnang_crawler.py: 중랑구 크롤러
    - gangdong_crawler.py: 강동구 크롤러
    - welfare_crawler.py: 서울시 복지포털 서비스 크롤러
    - ehealth_crawler.py: e보건소 크롤러
"""

from .base import BaseCrawler, LLMStructuredCrawler, HealthSupportInfo
from .crawlers import (
    DistrictCrawler,
    EHealthCrawler,
    WelfareCrawler,
    SongpaCrawler,
    YangcheonCrawler,
)
from .crawlers.specific_crawler import (
    district_menu_crawler,
    district_configs,
)

__all__ = [
    "BaseCrawler",
    "LLMStructuredCrawler",
    "HealthSupportInfo",
    "DistrictCrawler",
    "EHealthCrawler",
    "WelfareCrawler",
    "SongpaCrawler",
    "YangcheonCrawler",
    "district_menu_crawler",
    "district_configs",
]
