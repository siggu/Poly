"""
크롤러 베이스 클래스
"""

import requests
from bs4 import BeautifulSoup
from typing import Optional
from urllib.parse import urlparse
import sys
import os

# 상위 디렉토리의 config import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config


class BaseCrawler:
    """모든 크롤러의 기본 클래스"""

    def __init__(self, timeout: int = None):
        """
        Args:
            timeout: HTTP 요청 타임아웃 (초)
        """
        self.timeout = timeout or config.DEFAULT_TIMEOUT
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.DEFAULT_USER_AGENT})

    def _get_site_key(self, url: str) -> Optional[str]:
        """
        URL에서 사이트 키 추출 (사이트별 특수 설정용)

        Args:
            url: URL 문자열

        Returns:
            사이트 키 (예: "gangbuk", "gangseo") 또는 None
        """
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        for site_key in config.SITE_SPECIFIC_CONFIGS.keys():
            if site_key in domain:
                return site_key

        return None

    def _apply_site_specific_config(self, url: str):
        """
        사이트별 특수 설정 적용 (쿠키, SSL 등)

        Args:
            url: 요청할 URL

        Returns:
            verify_ssl: SSL 검증 여부
        """
        site_key = self._get_site_key(url)
        if not site_key:
            return True  # 기본값: SSL 검증 활성화

        site_config = config.SITE_SPECIFIC_CONFIGS.get(site_key, {})

        # 쿠키 설정
        if "cookies" in site_config:
            for key, value in site_config["cookies"].items():
                self.session.cookies.set(key, value)

        # SSL 경고 비활성화 (필요시)
        if site_config.get("disable_ssl_warnings", False):
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # SSL 검증 설정 반환
        return site_config.get("verify_ssl", True)

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        웹페이지 가져오기

        Args:
            url: 크롤링할 URL

        Returns:
            BeautifulSoup 객체 또는 None (실패 시)
        """
        import time
        start_time = time.time()

        try:
            # 사이트별 특수 설정 적용
            verify_ssl = self._apply_site_specific_config(url)

            # HTTP 요청 시간 측정
            http_start = time.time()
            response = self.session.get(url, timeout=self.timeout, verify=verify_ssl)
            response.raise_for_status()
            http_duration = time.time() - http_start

            # 인코딩 설정
            if response.apparent_encoding:
                response.encoding = response.apparent_encoding
            else:
                response.encoding = "utf-8"

            # HTML 파싱 시간 측정
            parse_start = time.time()
            soup = BeautifulSoup(response.text, "html.parser")
            parse_duration = time.time() - parse_start

            total_duration = time.time() - start_time

            # 속도 통계에 기록
            try:
                from app.crawling import utils
                utils.get_timing_stats().add_timing("1_HTTP요청", http_duration)
                utils.get_timing_stats().add_timing("2_HTML파싱", parse_duration)
                utils.get_timing_stats().add_timing("fetch_page_전체", total_duration)
            except:
                pass  # 통계 기록 실패해도 계속 진행

            return soup

        except requests.RequestException as e:
            print(f"  [오류] 페이지 요청 실패: {url} - {e}")
            return None

    def get(
        self, url: str, timeout: int = None, verify: bool = None
    ) -> Optional[requests.Response]:
        """
        HTTP GET 요청 (저수준 API)

        Args:
            url: 요청할 URL
            timeout: 타임아웃 (초)
            verify: SSL 검증 여부 (None이면 자동 설정)

        Returns:
            Response 객체 또는 None (실패 시)
        """
        try:
            # verify가 명시되지 않으면 사이트별 설정 적용
            if verify is None:
                verify = self._apply_site_specific_config(url)

            response = self.session.get(
                url, timeout=timeout or self.timeout, verify=verify
            )
            response.raise_for_status()
            return response

        except requests.RequestException as e:
            print(f"  [오류] HTTP GET 요청 실패: {url} - {e}")
            return None
