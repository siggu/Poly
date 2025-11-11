"""
링크 필터링 전담 클래스
키워드 기반 또는 LLM 기반 링크 필터링을 수행합니다.
"""

from typing import List, Dict


class LinkFilter:
    """링크 필터링 전담 클래스"""

    def __init__(self):
        pass

    def filter_by_keywords(
        self,
        links: List[Dict],
        whitelist: List[str] = None,
        blacklist: List[str] = None,
        mode: str = "both",
    ) -> List[Dict]:
        """
        키워드 기반 링크 필터링

        Args:
            links: 필터링할 링크 목록 [{"name": str, "url": str}, ...]
            whitelist: 포함되어야 하는 키워드 목록
            blacklist: 제외되어야 하는 키워드 목록
            mode: "whitelist", "blacklist", "both", "none"

        Returns:
            필터링된 링크 목록
        """
        if not links or mode == "none":
            return links

        print(f"\n[키워드 필터링] 총 {len(links)}개 링크를 '{mode}' 모드로 필터링 중...")

        filtered_links = []
        excluded_links = []

        for link in links:
            name = link["name"]
            passed, reason = self.check_keyword_filter(
                name, whitelist, blacklist, mode
            )

            if passed:
                filtered_links.append(link)
                print(f"  ✓ [포함] {name}")
            else:
                excluded_links.append({"name": name, "reason": reason})
                print(f"  ✗ [제외] {name} - {reason}")

        print(
            f"\n[키워드 필터링 완료] {len(links)}개 중 {len(filtered_links)}개 링크 선택됨 (제외: {len(excluded_links)}개)"
        )

        return filtered_links

    def check_keyword_filter(
        self,
        link_name: str,
        whitelist: List[str] = None,
        blacklist: List[str] = None,
        mode: str = "both",
    ) -> tuple[bool, str]:
        """
        단일 링크 이름에 대해 키워드 필터링 체크

        Args:
            link_name: 링크 이름
            whitelist: 포함되어야 하는 키워드 목록
            blacklist: 제외되어야 하는 키워드 목록
            mode: "whitelist", "blacklist", "both", "none"

        Returns:
            (통과 여부, 제외 이유) 튜플
        """
        if mode == "none":
            return True, ""

        # 화이트리스트 체크
        if mode in ["whitelist", "both"] and whitelist:
            if not any(keyword in link_name for keyword in whitelist):
                return False, "화이트리스트 키워드 없음"

        # 블랙리스트 체크
        if mode in ["blacklist", "both"] and blacklist:
            matched_blacklist = [
                keyword for keyword in blacklist if keyword in link_name
            ]
            if matched_blacklist:
                return False, f"블랙리스트 키워드 포함: {', '.join(matched_blacklist)}"

        return True, ""
