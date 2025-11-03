from urllib.parse import urlparse
import sys

def extract_sitename_from_url(url: str) -> str:
    """
    URL에서 사이트명 추출

    Args:
        url: URL 문자열

    Returns:
        지역명 (예: "강남구보건소", "동작구보건소 공지사항") 또는 "unknown"
    """
    region_mapping = {
        "gangnam": "강남구보건소",
        "gangdong": "강동구보건소",
        "gangbuk": "강북구보건소",
        "gangseo": "강서구보건소",
        "guro": "구로구보건소",
        "gwanak": "관악구보건소",
        "dongjak": "동작구보건소",
        "ddm": "동대문구보건소",
        "gwangjin": "광진구보건소",
        "nowon": "노원구보건소",
        "jongno": "종로구보건소",
        "yongsan": "용산구보건소",
        "junggu": "중구보건소",
        "dobong": "도봉구보건소",
        "mapo": "마포구보건소",
        "sdm": "서대문구보건소",
        "seocho": "서초구보건소",
        "sd": "성동구보건소",
        "sb": "성북구보건소",
        "songpa": "송파구보건소",
        "yangcheon": "양천구보건소",
        "ydp": "영등포구보건소",
        "seoul-agi": "서울시 임신-출산정보센터",
        "wis.seoul": "서울복지포털",
        "news.seoul": "서울특별시 공식사이트",
        "e-health": "e보건소",
        "bokjiro": "복지로",
        "nhis": "국민건강보험공단"
    }

    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()

    # 매핑 실패 시 도메인 첫 부분 반환
    return domain.split(".")[0] if "." in domain else "unknown"

# --------------------------------
# 3. 가중치 계산
# --------------------------------
def get_weight(region: str, sitename: str):
    if not region:
        return 1
    region = region.strip()
    if "전국" in region:
        return 2
    elif "서울" in region:
        if "서울복지포털" in sitename:
            return 3 # 서울복지포털(가중치 낮음)
        else:
            return 4
    else:
        if "공지사항" not in sitename:
            return 5 # 구 보건소
        else:
            return 6  # 구 보건소 공지사항

# --------------------------------
# 0. 유틸
# --------------------------------
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)