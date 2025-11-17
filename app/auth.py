from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

# 이제 DB Model 대신 TokenData 스키마만 가져옵니다.
from app.schemas import TokenData

# 설정 값 (실제 애플리케이션에서는 환경 변수나 설정 파일에서 불러와야 합니다.)
SECRET_KEY = "YOUR_SECRET_KEY"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """주어진 데이터로 JWT 액세스 토큰을 생성합니다."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
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


# NOTE: 데이터베이스 의존성 제거에 따라 get_db_session()과 get_current_active_user()는 제거되었습니다.
# 유저 객체 조회 및 is_active 검증과 같은 DB 관련 로직은
# 이제 라우터 파일(예: users/router.py) 내의 새로운 의존성 함수에서 처리해야 합니다.
