import sys
import os

# 공통 모듈 import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
import utils
from base.base_crawler import BaseCrawler


def main():
    """
    메인 크롤링 함수
    """
    # 1. 사용자 입력
    start_url = input("분석할 웹사이트 URL을 입력하세요: ")

    # 2. ⭐ 동적 base_url 생성
    try:
        base_url = utils.get_base_url(start_url)
        print(f"--- 0단계: 기본 URL을 '{base_url}' (으)로 설정합니다 ---")
    except ValueError as e:
        print(f"[오류] {e}")
        return

    # BaseCrawler 인스턴스 생성
    crawler = BaseCrawler()

    print("\n--- 1단계: 메인 카테고리 링크 수집 시작 ---")

    # 3. 시작 페이지 파싱
    soup = crawler.fetch_page(start_url)
    if not soup:
        print("시작 페이지에 접속할 수 없습니다. 스크립트를 종료합니다.")
        return

    # 4. ⭐ 일치하는 '규칙' 찾기
    main_links = []
    active_rule = None

    for rule in config.CRAWL_RULES:
        print(f"  [시도] 규칙 '{rule['name']}' (선택자: {rule['main_selector']})")
        main_links = soup.select(rule["main_selector"])
        if main_links:
            print(f"  [성공] 이 규칙으로 {len(main_links)}개의 링크를 찾았습니다.")
            active_rule = rule  # 사용된 규칙을 저장
            break

    if not active_rule:
        print("\n[오류] 1단계 메뉴 링크를 수집할 수 없습니다.")
        print("CRAWL_RULES에 정의된 'main_selector' 중 일치하는 것이 없습니다.")
        return

    # 5. 1단계 메뉴 링크 처리
    main_categories = []
    for link in main_links:
        category_name = link.get_text().strip()
        relative_href = link.get("href")

        # 상대 경로를 절대 경로로 변환
        absolute_url = utils.make_absolute_url(relative_href, base_url)

        main_categories.append({"name": category_name, "url": absolute_url})
        # print(f"  [수집] {category_name} ({absolute_url})") # 1단계 로그는 성공 로그로 대체

    print(
        f"\n--- 2단계: 총 {len(main_categories)}개의 카테고리를 순회하며 하위 메뉴 수집 ---"
    )

    # 6. 수집된 1단계 메뉴를 순회하며 각 페이지의 하위 메뉴 수집
    all_menus_data = {}

    for category in main_categories:
        print(f"\n[방문 중...] {category['name']} ({category['url']})")

        # 외부 링크(base_url로 시작하지 않는 링크)는 건너뛰기
        if not category["url"].startswith(base_url):
            print("  [알림] 외부 사이트이므로 건너뜁니다.")
            all_menus_data[category["name"]] = []
            continue

        category_soup = crawler.fetch_page(category["url"])
        if not category_soup:
            continue

        sub_menu_list = []

        # ⭐ 활성화된 규칙(active_rule)의 'sub_selector'를 사용
        found_sub_links = False

        for finder_selector in active_rule["policy_finders"]:
            sub_links = category_soup.select(finder_selector)

            if sub_links:
                # 하위 메뉴가 있으면(Case 1: LNB 또는 Tab), 하위 메뉴들을 수집
                print(
                    f"  [알림] (규칙: {finder_selector})에서 하위 메뉴 {len(sub_links)}개를 찾았습니다."
                )
                found_sub_links = True

                for sub_link in sub_links:
                    sub_name = sub_link.get_text().strip()
                    sub_href = utils.make_absolute_url(sub_link.get("href"), base_url)
                    sub_menu_list.append({"name": sub_name, "url": sub_href})

                break  # 하위 링크를 찾았으므로 다음 규칙(finder)은 확인할 필요 없음

        if not found_sub_links:
            # 하위 메뉴가 없으면(Case 2), 카테고리 자체를 단일 항목으로 간주
            print("  [알림] 하위 메뉴가 없습니다. 카테고리 자체를 항목으로 수집합니다.")
            sub_menu_list.append({"name": category["name"], "url": category["url"]})
        all_menus_data[category["name"]] = sub_menu_list

    # 7. 최종 결과 출력
    print("\n\n--- 🌟 최종 수집 결과 🌟 ---")
    for main_name, sub_menus in all_menus_data.items():
        print(f"\n■ {main_name}")
        if sub_menus:
            for sub in sub_menus:
                print(f"  - {sub['name']} ({sub['url']})")
        else:
            print("  (하위 메뉴 없음 또는 외부 링크)")


# --- 스크립트 실행 ---
if __name__ == "__main__":
    main()
