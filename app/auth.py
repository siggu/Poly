"""인증 관련 유틸리티 함수들 - DB 의존성 제거 버전 함수 수정 완료 11.18"""
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

# 이제 DB Model 대신 TokenData 스키마를 가져옴.
from app.schemas import TokenData

# 설정 값 (실제 애플리케이션에서는 환경 변수나 설정 파일에서 불러와야 합니다.)
SECRET_KEY = "YOUR_SECRET_KEY"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24시간 (1일)
REFRESH_TOKEN_EXPIRE_MINUTES = 60 * 24 * 30  # 30일

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """주어진 데이터로 JWT 액세스 토큰을 생성합니다."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# 11.17 추가: 리프레시 토큰 생성 함수
def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None):
    """주어진 데이터로 JWT 리프레시 토큰을 생성합니다."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=REFRESH_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire, "type": "refresh"})  # 리프레시 토큰임을 명시
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# get_current_user 함수가 데이터베이스 의존성(Session)을 제거하고
# 오직 JWT 토큰 검증과 TokenData 반환만 담당합니다.
async def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenData:
    """JWT 토큰을 검증하고, 유저네임이 포함된 TokenData를 반환합니다. 데이터베이스 접근은 없습니다."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials (token validation failed)",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # 1. 토큰 디코딩 및 검증
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        # 2. 이메일을 포함한 TokenData 반환
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    return token_data
# 이제 get_current_user 함수는 데이터베이스에 접근하지 않으며,
# 단순히 토큰을 검증하고 TokenData 객체를 반환하는 역할만 수행
