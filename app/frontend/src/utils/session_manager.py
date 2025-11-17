"""세션 관리 유틸리티 함수들 - 11.17 완전 수정 버전"""

import json
import logging
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


def save_session(user_info: Dict[str, Any], auth_token: str):
    """
    로그인 세션을 파일에 저장

    Args:
        user_info: 사용자 정보 딕셔너리
        auth_token: JWT 인증 토큰
    """
    session_file = get_session_file_path()

    # ✅ auth_token 포함하여 저장
    session_data = {
        "user_info": user_info,
        "auth_token": auth_token,  # ✅ 추가!
        "is_logged_in": True,
    }

    try:
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"✅ 세션 저장 완료 - user: {user_info.get('userId', 'unknown')}")
        logger.info(f"✅ 토큰 저장됨: {auth_token[:20]}...")
    except Exception as e:
        logger.error(f"❌ 세션 저장 실패: {e}")


def load_session() -> Optional[Dict[str, Any]]:
    """
    저장된 세션을 파일에서 로드

    Returns:
        세션 데이터 딕셔너리 (user_info, auth_token, is_logged_in 포함)
        또는 None (파일이 없거나 로드 실패 시)
    """
    session_file = get_session_file_path()

    if not session_file.exists():
        logger.warning("⚠️ 세션 파일이 존재하지 않습니다.")
        return None

    try:
        with open(session_file, "r", encoding="utf-8") as f:
            session_data = json.load(f)

        # ✅ 로드 확인 로그
        logger.info(f"✅ 세션 로드 완료")
        logger.info(f"   - is_logged_in: {session_data.get('is_logged_in')}")
        logger.info(f"   - auth_token 존재: {'auth_token' in session_data}")
        if "auth_token" in session_data:
            logger.info(f"   - 토큰: {session_data['auth_token'][:20]}...")

        return session_data
    except Exception as e:
        logger.error(f"❌ 세션 로드 실패: {e}")
        return None


def update_login_status(is_logged_in: bool = False):
    """
    로그인 상태만 업데이트

    Args:
        is_logged_in: 로그인 상태 (False면 로그아웃 처리)
    """
    try:
        session_data = load_session() or {}
        session_data["is_logged_in"] = is_logged_in

        # ✅ 로그아웃 시 토큰도 삭제
        if not is_logged_in:
            session_data["auth_token"] = None
            logger.info("🔓 로그아웃 처리 - 토큰 삭제됨")

        session_file = get_session_file_path()
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"✅ 로그인 상태 업데이트 완료: {is_logged_in}")
        return True
    except Exception as e:
        logger.error(f"❌ 로그인 상태 업데이트 실패: {e}")
        return False


def clear_session():
    """세션 파일 삭제"""
    session_file = get_session_file_path()
    try:
        if session_file.exists():
            session_file.unlink()
            logger.info("✅ 세션 파일 삭제 완료")
        else:
            logger.warning("⚠️ 삭제할 세션 파일이 없습니다.")
    except Exception as e:
        logger.error(f"❌ 세션 삭제 실패: {e}")
