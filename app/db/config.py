"""데이터베이스 연결 설정 - app.config의 Settings를 기반으로 DB_CONFIG 생성"""

import logging
from app.config import settings

logger = logging.getLogger(__name__)

DB_CONFIG = {}

# DATABASE_URL이 있으면 파싱, 없으면 개별 환경변수 사용
if settings.DATABASE_URL:
    from urllib.parse import urlparse
    db_url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    parsed = urlparse(db_url)
    DB_CONFIG = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path[1:] if parsed.path else "",
        "user": parsed.username,
        "password": parsed.password,
    }
else:
    # 개별 환경변수에서 DB 설정 로드
    required_keys = {
        "host": settings.DB_HOST,
        "port": settings.DB_PORT,
        "database": settings.DB_NAME,
        "user": settings.DB_USER,
        "password": settings.DB_PASSWORD,
    }
    for key, value in required_keys.items():
        if not value and key != "port":
            logger.error(f"필수 환경 변수 'DB_{key.upper()}'가 누락되었습니다.")
            raise EnvironmentError(
                f"필수 환경 변수 'DB_{key.upper()}'가 누락되었습니다. 프로그램을 중단합니다."
            )
    DB_CONFIG = {
        "host": settings.DB_HOST,
        "port": settings.DB_PORT,
        "database": settings.DB_NAME,
        "user": settings.DB_USER,
        "password": settings.DB_PASSWORD,
    }

logger.info("DB 환경 설정 로드 및 유효성 검사 성공.")
