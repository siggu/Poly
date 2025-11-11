"""
크롤러 공통 유틸리티 함수
"""

from urllib.parse import urlparse, urljoin, parse_qs
from typing import Optional, Dict, Set
import time
from functools import wraps
from contextlib import contextmanager


def extract_region_from_url(url: str) -> str:
    """
    URL에서 지역명 추출

    Args:
        url: URL 문자열

    Returns:
        지역명 (예: "강남구", "동작구") 또는 "unknown"
    """
    region_mapping = {
        "gangnam": "강남구",
        "gangdong": "강동구",
        "gangbuk": "강북구",
        "gangseo": "강서구",
        "guro": "구로구",
        "gwanak": "관악구",
        "dongjak": "동작구",
        "ddm": "동대문구",
        "gwangjin": "광진구",
        "nowon": "노원구",
        "jongno": "종로구",
        "yongsan": "용산구",
        "junggu": "중구",
        "dobong": "도봉구",
        "mapo": "마포구",
        "sdm": "서대문구",
        "seocho": "서초구",
        "sd": "성동구",
        "sb": "성북구",
        "songpa": "송파구",
        "yangcheon": "양천구",
        "ydp": "영등포구",
        "seoul-agi": "서울시",
        "wis.seoul": "서울시",
        "e-health": "전국",
    }

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    for key, value in region_mapping.items():
        if key in domain:
            return value

    # 매핑 실패 시 도메인 첫 부분 반환
    return domain.split(".")[0] if "." in domain else "unknown"


def get_base_url(url: str) -> str:
    """
    URL에서 base URL 추출 (scheme + netloc)

    Args:
        url: 전체 URL

    Returns:
        base URL (예: "https://example.com")

    Raises:
        ValueError: 유효하지 않은 URL인 경우
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"유효하지 않은 URL: {url}")

    return f"{parsed.scheme}://{parsed.netloc}"


def are_urls_equivalent(url1: str, url2: str) -> bool:
    """
    두 URL이 실질적으로 동일한지 비교 (정규화 후 비교)
    - scheme, netloc, path, query parameter를 모두 고려
    - fragment(#)는 무시
    """
    if not url1 or not url2:
        return False
    try:
        p1 = urlparse(url1)
        p2 = urlparse(url2)

        # scheme, netloc, path (trailing slash 무시) 비교
        if (
            p1.scheme.lower() != p2.scheme.lower()
            or p1.netloc.lower() != p2.netloc.lower()
            or p1.path.rstrip("/") != p2.path.rstrip("/")
        ):
            return False

        # Query parameter 순서와 상관없이 비교
        qs1 = parse_qs(p1.query)
        qs2 = parse_qs(p2.query)
        return qs1 == qs2
        # fragment는 비교하지 않음 (p1.fragment, p2.fragment 무시)
    except Exception:
        # 파싱 오류 발생 시, 기본적인 문자열 비교로 대체 (fragment 제거)
        cleaned_url1 = url1.split("#")[0].rstrip("/").lower()
        cleaned_url2 = url2.split("#")[0].rstrip("/").lower()
        return cleaned_url1 == cleaned_url2


def make_absolute_url(url: str, base_url: str) -> str:
    """
    상대 URL을 절대 URL로 변환

    Args:
        url: 상대 또는 절대 URL
        base_url: 기준이 되는 base URL

    Returns:
        절대 URL
    """
    # 이미 절대 URL이면 그대로 반환
    if url.startswith("http"):
        return url

    return urljoin(base_url, url)


def extract_link_from_element(
    link_element, base_url: str, seen_urls: Optional[Set[str]] = None
) -> Optional[Dict[str, str]]:
    """
    링크 요소에서 URL과 이름을 추출하고 검증

    Args:
        link_element: BeautifulSoup 링크 요소
        base_url: 기준 URL
        seen_urls: 이미 수집된 URL 집합 (None이면 중복 체크 안 함)

    Returns:
        {"name": str, "url": str} 또는 None (무효한 링크인 경우)
    """
    name = link_element.get_text(strip=True)
    href = link_element.get("href", "")

    if not href:
        return None

    # 절대 URL로 변환
    url = urljoin(base_url, href)

    # 중복 확인 (seen_urls가 제공된 경우에만)
    if seen_urls is not None and url in seen_urls:
        return None

    return {"name": name, "url": url}


# ============================================================
# 속도 측정 유틸리티
# ============================================================

class TimingStats:
    """속도 측정 통계를 저장하는 클래스"""
    def __init__(self):
        self.timings = {}

    def add_timing(self, category: str, duration: float):
        """특정 카테고리에 실행 시간 추가"""
        if category not in self.timings:
            self.timings[category] = []
        self.timings[category].append(duration)

    def get_stats(self, category: str) -> Dict:
        """특정 카테고리의 통계 반환"""
        if category not in self.timings or not self.timings[category]:
            return {"count": 0, "total": 0, "avg": 0, "min": 0, "max": 0}

        times = self.timings[category]
        return {
            "count": len(times),
            "total": sum(times),
            "avg": sum(times) / len(times),
            "min": min(times),
            "max": max(times)
        }

    def print_summary(self):
        """전체 통계 요약 출력"""
        print("\n" + "=" * 80)
        print("⏱️  속도 측정 결과 요약")
        print("=" * 80)

        for category in sorted(self.timings.keys()):
            stats = self.get_stats(category)
            print(f"\n[{category}]")
            print(f"  총 호출 횟수: {stats['count']}회")
            print(f"  총 소요 시간: {stats['total']:.2f}초")
            print(f"  평균 시간: {stats['avg']:.2f}초")
            print(f"  최소 시간: {stats['min']:.2f}초")
            print(f"  최대 시간: {stats['max']:.2f}초")

        print("=" * 80)


# 전역 통계 객체
_global_timing_stats = TimingStats()


def get_timing_stats() -> TimingStats:
    """전역 통계 객체 반환"""
    return _global_timing_stats


@contextmanager
def measure_time(category: str, description: str = None, verbose: bool = True):
    """
    코드 블록의 실행 시간을 측정하는 컨텍스트 매니저

    사용 예:
        with measure_time("HTTP요청", "페이지 가져오기"):
            response = requests.get(url)

    Args:
        category: 측정 카테고리 (통계 집계용)
        description: 출력할 설명 (None이면 출력 안 함)
        verbose: 측정 결과를 즉시 출력할지 여부
    """
    start_time = time.time()

    if description and verbose:
        print(f"    [⏱️ START] {description}...", end="", flush=True)

    try:
        yield
    finally:
        duration = time.time() - start_time
        _global_timing_stats.add_timing(category, duration)

        if description and verbose:
            print(f" 완료 ({duration:.2f}초)")


def timing_decorator(category: str):
    """
    함수 실행 시간을 측정하는 데코레이터

    사용 예:
        @timing_decorator("LLM호출")
        def call_llm_api():
            ...

    Args:
        category: 측정 카테고리 (통계 집계용)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                _global_timing_stats.add_timing(category, duration)
        return wrapper
    return decorator
