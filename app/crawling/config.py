"""
크롤러 설정 및 크롤링 규칙 정의
"""

# ========================================
# 크롤링 규칙 (CRAWL_RULES)
# ========================================
# 여러 보건소 사이트의 LNB 구조에 대응하는 규칙 목록
#
# 'name':         규칙의 이름 (로그에 표시됨)
# 'domain':       적용할 도메인 키워드
# 'single_page':  단일 페이지에서 모든 메뉴를 수집할지 여부
# 'filter_menu':  특정 메뉴만 필터링 (single_page=True일 때)
# 'menu_container': 메뉴 컨테이너 선택자 (single_page=True일 때)
# 'main_selector': 1단계 메인 메뉴 링크를 찾는 CSS 선택자
# 'sub_selector':  하위 메뉴 링크를 찾는 CSS 선택자

CRAWL_RULES = [
    {
        "name": "동작구 건강관리청 LNB",
        "domain": "dongjak",
        "main_selector": ".left-area .left-mdp1 > li > a",
        "sub_selector": [
            ".left-mdp1 > li.on > ul > li > a",
            ".tab-list li a",
        ],
    },
    {
        "name": "강남구보건소 LNB",
        "domain": "gangnam",
        "main_selector": ".left_menu_list > .oneDepth > a",
        "sub_selector": [
            ".oneDepth.active .twoDepth .oneDepth a",
            ".tabmenu ul li a",
        ],
    },
    {
        "name": "강북구 보건소 LNB",
        "domain": "gangbuk",
        "main_selector": ".lnb nav > ul > li > a",
        "sub_selector": [
            ".lnb nav ul li.on ul li a",
            ".tab-nav ul li a",
        ],
    },
    {
        "name": "강서구 보건소 LNB",
        "domain": "gangseo",
        "single_page": True,
        "menu_container": ".lnb-menu",
        "main_selector": "a[href]:not([href='#']):not([href=''])",
        "sub_selector": None,
    },
    {
        "name": "광진구 보건소 LNB",
        "domain": "gwangjin",
        "main_selector": "nav.lnb > ul > li > a",
        "sub_selector": [
            "nav.lnb ul li.on div ul li a",
            ".tab-list .item a",
        ],
    },
    {
        "name": "구로구 보건소 LNB",
        "domain": "guro",
        "single_page": True,
        "menu_container": ".side_menu nav.menu",
        "main_selector": ".depth2_list > .depth2_item > a.depth2_text",
        "sub_selector": ".depth3_list > .depth3_item > a.depth3_text",
    },
    {
        "name": "도봉구 보건소 LNB",
        "domain": "dobong",
        "single_page": True,
        "menu_container": ".s_con_left",
        "main_selector": "ul.depth1 > li > a",
        "sub_selector": "ul.depth2 > li > a",
    },
    {
        "name": "서대문구 보건소 LNB",
        "domain": "sdm",
        "single_page": True,
        "main_selector": "ul.depth03 > li > a:not([href^='/health/htmlView/html'])",
        "sub_selector": None,
    },
    {
        "name": "서초구 보건소 LNB",
        "domain": "seocho",
        "single_page": True,
        "menu_container": "#snav nav",
        "main_selector": "ul.dep2 > li > a",
        "sub_selector": "ul.dep3 > li > a",
    },
    {
        "name": "성북구 보건소 LNB",
        "domain": "sb",
        "single_page": True,
        "filter_menu": "사업안내",
        "menu_container": ".side_menu nav.menu",
        "main_selector": ".depth1_list > .depth1_item > a.depth1_text",
        "sub_selector": ".depth2_list > .depth2_item > a.depth2_text",
    },
    {
        "name": "송파구 보건소 LNB",
        "domain": "songpa",
        "single_page": True,
        "menu_container": ".side_menu",
        "main_selector": "a.depth_text[href*='contents.do'][target='_self']",
        "sub_selector": None,
    },
    {
        "name": "양천구 보건소 LNB",
        "domain": "yangcheon",
        "single_page": True,
        "filter_menu": "보건사업",
        "menu_container": ".subnav-dep2",
        "main_selector": "ul li a",
        "sub_selector": None,
    },
    {
        "name": "노원구 보건소 LNB",
        "domain": "nowon",
        "single_page": True,
        "menu_container": "#snb",
        "main_selector": "a[href^='/health/']",
        "sub_selector": None,
    },
    {
        "name": "서울시 임신출산정보센터",
        "domain": "seoul-agi",
        "single_page": True,
        "menu_container": "#content-menu",
        "main_selector": ".menu-item > .menu-link",
        "sub_selector": ".menu-sub-item > a.menu-sub-link",
    },
]

# ========================================
# 탭 메뉴 선택자
# ========================================
# 페이지 내부의 탭 메뉴를 찾는 데 사용할 CSS 선택자 목록
TAB_SELECTORS = [
    ".tabmenu ul li a",  # 강남구 등
    ".tab-list li a",  # 동작구
    ".tab-nav ul li a",  # 강북구
    "ul.tab-wrap li a",  # 강서구
    ".tab-list .item a",  # 광진구
    ".tab1 ul li a",  # 도봉구
    "ul.sub-tab li a",  # 서대문구
    ".tab-wrap ul li a",  # 서초구
    ".tab_panel ul li a",  # 성북구
    ".tab_panel ul li button",  # 성북구
    ".content-tab ul li a",  # 양천구
    ".nw-tab-bx .nw-tab-ls > li > p > a",  # 추가 탭 패턴
]

# ========================================
# HTTP 설정
# ========================================
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DEFAULT_TIMEOUT = 15  # 초
DEFAULT_DELAY = 1  # 요청 간 지연 시간 (초)
RATE_LIMIT_DELAY = 0.5  # Rate limiting 지연 시간 (초)

# ========================================
# 사이트별 특수 설정
# ========================================
# 특정 사이트에 필요한 쿠키, SSL 설정 등
SITE_SPECIFIC_CONFIGS = {
    "gangbuk": {
        "cookies": {
            "sabFingerPrint": "1920,1080,www.gangbuk.go.kr",
            "sabSignature": "f3m9iqAXBBmHE38diIuA0A==",
        },
        "verify_ssl": True,
    },
    "gangseo": {
        "verify_ssl": False,
        "disable_ssl_warnings": True,
    },
    "yangcheon": {
        "verify_ssl": False,
        "disable_ssl_warnings": True,
    },
}

# ========================================
# 출력 설정
# ========================================
DEFAULT_OUTPUT_DIR = "crawling/output"

# ========================================
# e보건소 설정
# ========================================
EHEALTH_CATEGORIES = {
    "건강증진": {"bbsSeCd": "Z1", "menuId": "200035"},
    "질병관리": {"bbsSeCd": "Z2", "menuId": "200036"},
    "암관리": {"bbsSeCd": "Z3", "menuId": "200037"},
    "구강보건": {"bbsSeCd": "Z4", "menuId": "200038"},
    "정신보건": {"bbsSeCd": "Z5", "menuId": "200039"},
    "가족건강": {"bbsSeCd": "Z6", "menuId": "200040"},
    "한의약": {"bbsSeCd": "Z7", "menuId": "200041"},
    "방문건강관리": {"bbsSeCd": "Z8", "menuId": "200091"},
}

EHEALTH_BASE_URL = "https://www.e-health.go.kr"
EHEALTH_BBS_ID = "U00322"

# ========================================
# 키워드 필터링 설정
# ========================================
# 화이트리스트: 제목에 이 키워드 중 하나라도 포함되면 수집
# 블랙리스트: 제목에 이 키워드 중 하나라도 포함되면 제외
# mode: "whitelist" (화이트리스트만), "blacklist" (블랙리스트만), "both" (둘 다), "none" (비활성화)
KEYWORD_FILTER = {
    "whitelist": [
        # 건강/의료 관련 키워드
        "지원",
        "건강",
        "금연",
        "비만",
        "노인",
        "암",
        "영유아",
        "건강검진",
        "구강",
        "예방접종",
        "검진",
        "임산",
        "임신",
        "청소년",
        "진료",
        "불소",
        "난치성",
        "영양",
    ],
    "blacklist": [
        # 비건강/의료 관련 키워드
        "교육",
        "교육비",
        "센터",
        "캠페인",
        "건강도시",
        "조사",
        # "대여",
        "신고",
        # "등록",
        "동영상",
        "커뮤니티",
        "게시판",
        "의견제출",
        "위치",
        "방역",
        "뉴스레터",
        "소식지",
        "현황",
        "단점",
        "시설",
        "시스템",
        "안전도시",
        "동물",
        "사진",
        "자원",
        "종류",
        "식품",
        "기관정보",
        "자가검진",
        "자료실",
        "정의",
    ],
    "mode": "blacklist",  # "whitelist", "blacklist", "both", "none"
}
