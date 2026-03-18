"""
필드 매핑 통합 모듈
한국어 ↔ DB enum 간 변환 매핑을 한 곳에서 관리합니다.
"""

# ── 성별 (Gender) ──
GENDER_TO_DB = {"남성": "M", "여성": "F"}
GENDER_FROM_DB = {v: k for k, v in GENDER_TO_DB.items()}

# ── 건강보험 유형 (Insurance Type) ──
INSURANCE_TO_DB = {
    "직장": "EMPLOYED",
    "지역": "REGIONAL",
    "피부양": "DEPENDENT",
    "의료급여": "MEDICAL",
}
INSURANCE_FROM_DB = {v: k for k, v in INSURANCE_TO_DB.items()}
INSURANCE_LABELS = {
    "EMPLOYED": "직장가입자",
    "REGIONAL": "지역가입자",
    "DEPENDENT": "피부양자",
    "MEDICAL": "의료급여",
}

# ── 기초생활수급 (Basic Benefit Type) ──
BENEFIT_TO_DB = {
    "없음": "NONE",
    "생계": "LIVELIHOOD",
    "의료": "MEDICAL",
    "주거": "HOUSING",
    "교육": "EDUCATION",
}
BENEFIT_FROM_DB = {v: k for k, v in BENEFIT_TO_DB.items()}
BENEFIT_LABELS = {
    "NONE": "비수급",
    "LIVELIHOOD": "생계급여",
    "MEDICAL": "의료급여",
    "HOUSING": "주거급여",
    "EDUCATION": "교육급여",
}

# ── 장애 등급 (Disability Grade) ──
DISABILITY_TO_DB = {"미등록": 0, "심한 장애": 1, "심하지 않은 장애": 2}
DISABILITY_FROM_DB = {0: "미등록", 1: "심한 장애", 2: "심하지 않은 장애"}
# 프론트엔드용 (문자열 키)
DISABILITY_STR_TO_DB = {"0": None, "1": 1, "2": 2}
DISABILITY_DB_TO_STR = {None: "0", 1: "1", 2: "2"}

# ── 임신 상태 (Pregnancy Status) ──
PREGNANCY_TO_DB = {"없음": False, "임신중": True, "출산후12개월이내": True}
PREGNANCY_FROM_DB = {False: "없음", True: "임신중"}

# ── 장기요양등급 (LTCI Grade) ──
LTCI_LABELS = {
    0: "미등록",
    1: "1등급",
    2: "2등급",
    3: "3등급",
    4: "4등급",
    5: "5등급",
}
