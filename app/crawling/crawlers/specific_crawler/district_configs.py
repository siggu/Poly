"""
구별 크롤링 설정

각 구별로 사용할 strategy와 시작 URL, 출력 디렉토리 등을 정의합니다.
"""

DISTRICT_CONFIGS = {
    "은평구": {
        "strategy": "ep_strategy",
        "start_url": "https://health.ep.go.kr/health/healthgongbo/0101",
        "output_dir": "app/crawling/output/은평구",
    },
    "강동구": {
        "strategy": "gangdong_strategy",
        "start_url": "https://health.gangdong.go.kr/health/health/01/",
        "output_dir": "app/crawling/output/강동구",
    },
    "종로구": {
        "strategy": "jongno_strategy",
        "start_url": "https://health.jongno.go.kr/main/health/health01.do",
        "output_dir": "app/crawling/output/종로구",
    },
    "중랑구": {
        "strategy": "jungnang_strategy",
        "start_url": "https://health.jungnang.go.kr/health/health01/health01_01.do",
        "output_dir": "app/crawling/output/중랑구",
    },
    "영등포구": {
        "strategy": "ydp_strategy",
        "start_url": "https://health.ydp.go.kr/health/health01/health01_01",
        "output_dir": "app/crawling/output/영등포구",
    },
    "용산구": {
        "strategy": "yongsan_strategy",
        "start_url": "https://health.yongsan.go.kr/health/health01/health01_01.do",
        "output_dir": "app/crawling/output/용산구",
    },
}
