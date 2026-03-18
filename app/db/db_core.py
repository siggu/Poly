"""데이터베이스 핵심 연결 기능"""

import psycopg2
import psycopg2.extras
import logging
from contextlib import contextmanager
from .config import DB_CONFIG

logger = logging.getLogger(__name__)

# UUID 어댑터 등록 (모듈 로드 시 한 번만 실행)
psycopg2.extras.register_uuid()


def get_db_connection():
    """PostgreSQL DB 연결 객체를 반환합니다."""
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            client_encoding="UTF8",
        )
        return conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        return None


@contextmanager
def get_db_context():
    """컨텍스트 매니저 패턴의 DB 커넥션"""
    conn = None
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            client_encoding="UTF8",
        )
        yield conn
    except psycopg2.OperationalError as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        yield None
    except Exception as e:
        logger.error(f"데이터베이스 오류: {e}")
        yield None
    finally:
        if conn:
            conn.close()
