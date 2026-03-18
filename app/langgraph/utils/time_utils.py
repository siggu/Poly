"""시간 유틸리티"""
from datetime import datetime, timezone


def now_iso() -> str:
    """현재 UTC 시각을 ISO 형식 문자열로 반환"""
    return datetime.now(timezone.utc).isoformat()
