"""
워크플로우 크롤러 베이스 클래스

템플릿 메서드 패턴을 사용하여 크롤링 워크플로우를 정의합니다.
하위 클래스는 각 단계를 오버라이드하여 커스터마이징할 수 있습니다.
"""

from typing import Dict, List, Sequence, Any, Tuple
from .parallel_crawler import BaseParallelCrawler


class WorkflowCrawler(BaseParallelCrawler):
    """
    워크플로우 기반 크롤러

    템플릿 메서드 패턴을 구현하여 다음 단계로 크롤링을 진행합니다:
    1. collect_initial_items: 초기 항목(링크) 수집
    2. save_initial_items: 초기 항목 저장 (선택)
    3. process_items_for_workflow: 항목 처리 및 구조화
    4. persist_results: 결과 저장
    5. print_workflow_summary: 요약 출력
    6. on_workflow_complete: 완료 후 처리
    """

    def run(
        self,
        *,
        start_url: str,
        save_initial: bool = True,
        save_json: bool = True,
        return_data: bool = False,
        **kwargs: Any,
    ) -> Dict:
        """
        전체 워크플로우 실행 (템플릿 메서드)

        Args:
            start_url: 시작 URL
            save_initial: 초기 항목 저장 여부
            save_json: JSON 저장 여부
            return_data: 결과 데이터를 반환할지 여부
            **kwargs: 하위 클래스 전용 추가 인자

        Returns:
            워크플로우 요약 정보
        """
        import time

        workflow_start = time.time()

        # 1. 초기 항목 수집
        initial_items = self.collect_initial_items(start_url=start_url, **kwargs)

        # 2. 초기 항목 저장 (선택)
        if save_initial and initial_items:
            self.save_initial_items(start_url=start_url, items=initial_items, **kwargs)

        # 3. 항목 처리 (크롤링 및 구조화)
        structured_items, failed_items, processed_count = (
            self.process_items_for_workflow(initial_items=initial_items, **kwargs)
        )

        # 4. 결과 저장
        summary = self.persist_results(
            start_url=start_url,
            initial_items=initial_items,
            structured_items=structured_items,
            failed_items=failed_items,
            processed_count=processed_count,
            save_json=save_json,
            return_data=return_data,
            **kwargs,
        )

        # 5. 요약 출력
        self.print_workflow_summary(
            initial_items=initial_items,
            processed_count=processed_count,
            structured_items=structured_items,
            failed_items=failed_items,
            **kwargs,
        )

        # 6. 완료 후 처리
        workflow_duration = time.time() - workflow_start
        self.on_workflow_complete(
            initial_items=initial_items,
            structured_items=structured_items,
            failed_items=failed_items,
            summary=summary,
            workflow_duration=workflow_duration,
            **kwargs,
        )

        return summary

    # ========== 하위 클래스에서 구현해야 하는 추상 메서드들 ==========

    def collect_initial_items(self, *, start_url: str, **kwargs: Any) -> List[Dict]:
        """
        초기 항목 수집 (예: 링크 목록)

        하위 클래스에서 반드시 구현해야 합니다.

        Returns:
            수집된 항목 리스트
        """
        raise NotImplementedError(
            "collect_initial_items() must be implemented by subclass"
        )

    def process_items_for_workflow(
        self, *, initial_items: Sequence[Dict], **kwargs: Any
    ) -> Tuple[List[Dict], List[Dict], int]:
        """
        항목 처리 (크롤링 및 구조화)

        하위 클래스에서 반드시 구현해야 합니다.

        Returns:
            (structured_items, failed_items, processed_count) 튜플
        """
        raise NotImplementedError(
            "process_items_for_workflow() must be implemented by subclass"
        )

    def persist_results(
        self,
        *,
        start_url: str,
        initial_items: Sequence[Dict],
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        processed_count: int,
        save_json: bool = True,
        return_data: bool = False,
        **kwargs: Any,
    ) -> Dict:
        """
        결과 저장

        하위 클래스에서 반드시 구현해야 합니다.

        Returns:
            요약 정보 딕셔너리
        """
        raise NotImplementedError("persist_results() must be implemented by subclass")

    # ========== 선택적으로 오버라이드할 수 있는 메서드들 ==========

    def save_initial_items(
        self, *, start_url: str, items: Sequence[Dict], **kwargs: Any
    ) -> None:
        """
        초기 항목 저장 (선택)

        기본 구현: 아무것도 하지 않음
        하위 클래스에서 필요시 오버라이드
        """
        pass

    def print_workflow_summary(
        self,
        *,
        initial_items: Sequence[Dict],
        processed_count: int,
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        **kwargs: Any,
    ) -> None:
        """
        워크플로우 요약 출력

        기본 구현: 간단한 통계 출력
        하위 클래스에서 필요시 오버라이드
        """
        print("\n" + "=" * 80)
        print("워크플로우 완료")
        print("=" * 80)
        print(f"초기 항목: {len(initial_items)}개")
        print(f"처리 시도: {processed_count}개")
        print(f"성공: {len(structured_items)}개")
        print(f"실패: {len(failed_items)}개")
        print("=" * 80)

    def on_workflow_complete(
        self,
        *,
        initial_items: Sequence[Dict],
        structured_items: Sequence[Dict],
        failed_items: Sequence[Dict],
        summary: Dict,
        workflow_duration: float,
        **kwargs: Any,
    ) -> None:
        """
        워크플로우 완료 후 처리

        기본 구현: 아무것도 하지 않음
        하위 클래스에서 필요시 오버라이드 (예: 통계, 정리 작업)
        """
        pass
