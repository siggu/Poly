"""11.12 환경 변수 로드 및 데이터베이스 연결 설정 및 유효성 검사"""

import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# DB 설정 가져오기 (기본값 제거)
raw_db_config = {
    "host": os.getenv("DB_HOST"),
    # DB_PORT 기본값 제거. 환경 변수가 없으면 None이 반환됩니다.
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    # DB_PASSWORD도 기본값 제거
    "password": os.getenv("DB_PASSWORD"),
}

DB_CONFIG = {}  # 유효성 검사를 통과한 최종 설정을 저장할 딕셔너리

# 필수 환경 변수 검사 및 타입 변환 (Fail Fast 원칙)
for key, value in raw_db_config.items():
    if value is None or (isinstance(value, str) and value.strip() == ""):
        logger.error(f"필수 환경 변수 'DB_{key.upper()}'가 누락되었습니다.")
        raise EnvironmentError(
            f"필수 환경 변수 'DB_{key.upper()}'가 누락되었습니다. 프로그램을 중단합니다."
        )

    # Port는 정수로 변환 시도
    if key == "port":
        try:
            DB_CONFIG[key] = int(value)
        except ValueError:
            logger.error(
                f"환경 변수 'DB_PORT'의 값 '{value}'는 유효한 정수가 아닙니다."
            )
            raise EnvironmentError("DB_PORT 환경 변수 오류. 유효한 정수여야 합니다.")
    else:
        DB_CONFIG[key] = value

logger.info("DB 환경 설정 로드 및 유효성 검사 성공.")
# 이제 DB_CONFIG는 모든 필수 설정이 포함된 안전한 딕셔너리입니다.