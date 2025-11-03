"""
여러 보건소 URL을 순회하며 district_crawler의 워크플로우를 실행하는 스크립트
"""

import os
import sys
import traceback

# 이 파일의 위치(app/interface/crawling/crawlers)를 기준으로
# 프로젝트 최상위 경로(HealthInformer)를 찾아 시스템 경로에 추가합니다.
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
sys.path.insert(0, project_root)

try:
    # 이제 최상위 경로가 포함되었으므로, 절대 경로로 임포트합니다.
    from app.crawling.crawlers.district_crawler import HealthCareWorkflow
    from app.crawling import utils
except ImportError as e:
    print("=" * 80)
    print("오류: 필요한 모듈을 임포트할 수 없습니다.")
    print("스크립트의 위치나 프로젝트 구조가 변경되었는지 확인하세요.")
    print(f"상세 오류: {e}")
    print("=" * 80)
    sys.exit(1)


def run_batch_crawling():
    """
    지정된 보건소 URL 목록을 순회하며 크롤링을 실행하고,
    각 결과를 지역별로 분리된 디렉터리에 저장합니다.
    """
    # =================================================================
    # 크롤링할 보건소의 '보건사업' 또는 유사한 메뉴의 시작 URL 목록
    # 여기에 다른 보건소 URL을 추가하거나 수정할 수 있습니다.
    # =================================================================
    target_urls = [
        "https://health.gangnam.go.kr/web/business/support/sub01.do",  # 강남구
        "https://health.gangdong.go.kr/health/site/main/content/GD20030100",  # 강동구
        "https://www.gangbuk.go.kr/health/main/contents.do?menuNo=400151",  # 강북구
        "https://www.gangseo.seoul.kr/health/ht020231",  # 강서구
        "https://www.gwanak.go.kr/site/health/05/10502010600002024101710.jsp",  # 관악구
        "https://www.gwangjin.go.kr/health/main/contents.do?menuNo=300080",  # 광진구
        "https://www.guro.go.kr/health/contents.do?key=1320&",  # 구로구
        "https://www.dongjak.go.kr/healthcare/main/contents.do?menuNo=300342",  # 동작구
        "https://www.sdm.go.kr/health/contents/infectious/law",  # 서대문구
        "https://www.seocho.go.kr/site/sh/03/10301000000002015070902.jsp",  # 서초구
        "https://www.sb.go.kr/bogunso/contents.do?key=6553",  # 성북구
        "https://www.ydp.go.kr/health/contents.do?key=6073&",  # 영등포구
        "https://www.songpa.go.kr/ehealth/contents.do?key=4525&",  # 송파구
        "https://jongno.go.kr/Health.do?menuId=401309&menuNo=401309",  # 종로구
    ]

    # 절대 경로를 사용하여 output 디렉토리 위치를 명확히 지정합니다.
    base_output_dir = os.path.join(
        project_root, "app", "interface", "crawling", "output"
    )
    print(f"총 {len(target_urls)}개의 보건소에 대한 크롤링을 시작합니다.")
    print("=" * 80)

    # 각 URL에 대해 워크플로우 실행
    for i, url in enumerate(target_urls, 1):
        try:
            # URL에서 지역명 추출
            region_name = utils.extract_region_from_url(url)
            if not region_name or region_name == "unknown":
                print(
                    f"[{i}/{len(target_urls)}] 경고: {url} 에서 지역명을 추출할 수 없습니다. 'unknown_region_{i}'으로 처리합니다."
                )
                region_name = f"unknown_region_{i}"

            print(f"[{i}/{len(target_urls)}] '{region_name}' 보건소 워크플로우 시작...")
            print(f"  - URL: {url}")

            # 결과를 저장할 지역별 출력 디렉토리 설정
            output_dir_for_region = os.path.join(base_output_dir, region_name)
            os.makedirs(output_dir_for_region, exist_ok=True)

            # 워크플로우 인스턴스 생성 및 실행
            workflow = HealthCareWorkflow(
                output_dir=output_dir_for_region, region=region_name
            )
            summary = workflow.run(start_url=url)

            print(f"[{i}/{len(target_urls)}] '{region_name}' 보건소 워크플로우 완료.")
            if summary:
                print(
                    f"  - 결과 요약: {summary.get('successful_structured', 0)}개 성공, {summary.get('failed_processing', 0)}개 실패"
                )
            else:
                print("  - 요약 정보를 가져오지 못했습니다.")
            print("-" * 80)

        except Exception as e:
            print(f"[{i}/{len(target_urls)}] URL {url} 처리 중 심각한 오류 발생: {e}")
            traceback.print_exc()
            print("-" * 80)

    print("모든 보건소 크롤링 작업이 완료되었습니다.")


if __name__ == "__main__":
    run_batch_crawling()
