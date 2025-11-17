"""세션 관리 유틸리티 함수들 11.13 수정"""

import json
import logging

# import os
from pathlib import Path
from typing import Optional, Dict, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_session_file_path() -> Path:
    """세션 파일 경로 반환"""
    base_dir = Path(__file__).parent.parent.parent
    session_dir = base_dir / ".session"
    session_dir.mkdir(exist_ok=True)
    return session_dir / "user_session.json"


def save_session(user_id: str, user_info: Dict[str, Any]):
    """로그인 세션을 파일에 저장"""
    session_file = get_session_file_path()
    session_data = {"user_id": user_id, "user_info": user_info, "is_logged_in": True}

    try:
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"세션 저장 실패: {e}")


# def save_session(
#     user_id: str = None, user_info: Dict[str, Any] = None, is_logged_in: bool = True
# ):
#     """
#     로그인 세션을 파일에 저장
#     :param user_id: 사용자 ID (없으면 기존 값 유지)
#     :param user_info: 사용자 정보 (없으면 기존 값 유지)
#     :param is_logged_in: 로그인 상태
#     """
#     session_file = get_session_file_path()

#     # 기존 세션 데이터 로드
#     existing_data = {}
#     if session_file.exists():
#         try:
#             with open(session_file, "r", encoding="utf-8") as f:
#                 existing_data = json.load(f)
#         except Exception as e:
#             logger.error(f"기존 세션 로드 실패: {e}")

#     # 새 데이터로 업데이트
#     session_data = {
#         "user_id": user_id if user_id is not None else existing_data.get("user_id"),
#         "user_info": (
#             user_info if user_info is not None else existing_data.get("user_info", {})
#         ),
#         "is_logged_in": is_logged_in,
#     }

#     try:
#         with open(session_file, "w", encoding="utf-8") as f:
#             json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)
#         logger.info("세션 저장 완료")
#     except Exception as e:
#         logger.error(f"세션 저장 실패: {e}")


def load_session() -> Optional[Dict[str, Any]]:
    """저장된 세션을 파일에서 로드"""
    session_file = get_session_file_path()

    if not session_file.exists():
        return None

    try:
        with open(session_file, "r", encoding="utf-8") as f:
            session_data = json.load(f)
        return session_data
    except Exception as e:
        print(f"세션 로드 실패: {e}")
        return None


def update_login_status(is_logged_in: bool = False):
    """로그인 상태만 업데이트"""
    try:
        # 기존 세션 데이터를 유지하면서 로그인 상태만 변경
        session_data = load_session() or {}
        session_data["is_logged_in"] = is_logged_in

        session_file = get_session_file_path()
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"로그인 상태 업데이트 완료: {is_logged_in}")
        return True
    except Exception as e:
        logger.error(f"로그인 상태 업데이트 실패: {e}")
        return False


def clear_session():
    """세션 파일 삭제"""
    session_file = get_session_file_path()
    try:
        if session_file.exists():
            session_file.unlink()
            logger.info("세션 파일 삭제 완료")
    except Exception as e:
        logger.error(f"세션 삭제 실패: {e}")
