"""User & Auth 관련 API 엔드포인트 -11.18(리프레시 토큰 수정, 프로필 필드명 변환 적용)"""

from fastapi import APIRouter, Depends, HTTPException, status
from passlib.context import CryptContext

from app.auth import create_access_token, create_refresh_token, get_current_user
from app.db import database as db_ops
from app.schemas import (
    UserCreate,
    UserLogin,
    UserProfile,
    UserProfileWithId,
    Token,
    TokenData,
    SuccessResponse,
    PasswordChangeRequest,
    RefreshTokenRequest,
    User,
)

router = APIRouter(
    prefix="/user",
    tags=["User & Auth"],
    responses={404: {"description": "Not found"}},
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ===============================================
# 의존성 함수
# ===============================================


def get_current_active_user(
    token_data: TokenData = Depends(get_current_user),
) -> dict:
    """
    유효한 토큰으로부터 DB에서 현재 활성화된 사용자 객체를 조회합니다.
    """
    if token_data.username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="토큰에 사용자 정보가 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # ✅ username으로 user_uuid 조회
    user_uuid = db_ops.get_user_uuid_by_username(token_data.username)
    if user_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자를 찾을 수 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # ✅ user_uuid로 사용자 정보 조회
    ok, user_info = db_ops.get_user_and_profile_by_id(user_uuid)
    if not ok or user_info is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자 정보를 가져올 수 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # user_info 딕셔너리에 'id' 키가 사용자 UUID를 담도록 보장합니다.
    # get_user_and_profile_by_id가 반환하는 딕셔너리의 'user_uuid' 키를 'id'로 매핑합니다.
    if user_info and "user_uuid" in user_info:
        user_info["id"] = user_info["user_uuid"]
    return user_info


# ===============================================
# 인증 엔드포인트
# ===============================================


@router.post(
    "/register", response_model=SuccessResponse, status_code=status.HTTP_201_CREATED
)
async def register_user(user_data: UserCreate):
    """회원가입"""
    if db_ops.check_user_exists(user_data.username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 존재하는 아이디입니다.",
        )

    hashed_password = pwd_context.hash(user_data.password)
    full_user_data = user_data.model_dump()
    full_user_data["password_hash"] = hashed_password

    ok, message = db_ops.create_user_and_profile(full_user_data)

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    return SuccessResponse(message=message)


@router.post("/login", response_model=Token, summary="사용자 로그인")
async def login_user(user_data: UserLogin):
    """로그인 및 JWT 토큰 발급"""
    stored_hash = db_ops.get_user_password_hash(user_data.username)

    if not stored_hash or not pwd_context.verify(user_data.password, stored_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="잘못된 아이디 또는 비밀번호입니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 1. 액세스 토큰 생성
    access_token = create_access_token(data={"sub": user_data.username})

    # 2. 리프레시 토큰 생성 및 DB 저장
    user_uuid = db_ops.get_user_uuid_by_username(user_data.username)
    if not user_uuid:
        raise HTTPException(status_code=500, detail="사용자 UUID를 찾을 수 없습니다.")

    # DB 저장 없이 리프레시 토큰 생성
    refresh_token = create_refresh_token(data={"sub": user_data.username})

    return Token(
        access_token=access_token,
        token_type="bearer",
        refresh_token=refresh_token,
    )


@router.get(
    "/check-id/{username}", response_model=SuccessResponse, summary="아이디 중복 확인"
)
async def check_id_availability(username: str):
    """
    주어진 아이디(username)가 이미 데이터베이스에 존재하는지 확인합니다.
    """
    if not username or len(username) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="아이디는 2자 이상이어야 합니다.",
        )
    if db_ops.check_user_exists(username):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 사용 중인 아이디입니다.",
        )
    return SuccessResponse(message="사용 가능한 아이디입니다.")


# ===============================================
# 프로필 엔드포인트
# ===============================================


# 11.18 수정 - 프로필 관련 API 엔드포인트에 필드명 변환 적용
# ✅ 프로필 조회 - DB 데이터를 프론트엔드 형식으로 변환
@router.get("/profile", summary="현재 사용자의 메인 프로필 조회")
async def get_user_profile(current_user: dict = Depends(get_current_active_user)):
    """
    인증된 사용자의 메인 프로필 정보를 조회합니다.
    ✅ DB 필드명 → 프론트엔드 필드명 변환
    """
    user_uuid = current_user.get("id")

    # DB에서 프로필 조회
    ok, profile_data = db_ops.get_user_and_profile_by_id(user_uuid)

    if not ok or not profile_data:
        raise HTTPException(status_code=404, detail="프로필을 찾을 수 없습니다.")

    return profile_data


# 11.18 수정
# ✅ 프로필 수정 - 필드명 변환 적용
@router.patch(
    "/profile/{profile_id}", response_model=SuccessResponse, summary="특정 프로필 수정"
)
async def update_user_profile(
    profile_id: int,
    update_data: UserProfile,
    current_user: dict = Depends(get_current_active_user),
):
    """특정 프로필 정보 수정 (필드명 변환 적용)"""
    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    # ✅ 프론트엔드 필드명 → DB 필드명 변환
    db_data = update_data.to_db_dict()

    # None 값 제거 (업데이트하지 않을 필드)
    db_data = {k: v for k, v in db_data.items() if v is not None}

    if not db_data:
        return SuccessResponse(message="수정할 내용이 없습니다.")

    if db_ops.update_profile(profile_id, db_data):
        return SuccessResponse(message="프로필이 성공적으로 수정되었습니다.")
    else:
        raise HTTPException(status_code=500, detail="프로필 수정에 실패했습니다.")


# 11.18수정
# ✅ 모든 프로필 조회 - DB 데이터를 프론트엔드 형식으로 변환
@router.get("/profiles", summary="사용자의 모든 프로필 조회")
async def get_all_user_profiles(
    current_user: dict = Depends(get_current_active_user),
):
    """사용자의 모든 프로필 조회 (필드명 변환 적용)"""
    user_uuid = current_user.get("id")

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, profiles_list = db_ops.get_all_profiles_by_user_id(user_uuid)

    if not ok:
        raise HTTPException(status_code=500, detail="프로필 조회에 실패했습니다.")

    # ✅ 각 프로필의 DB 필드명 → 프론트엔드 필드명 변환
    frontend_profiles = []
    for profile in profiles_list:
        frontend_profile = UserProfile.from_db_dict(profile)
        frontend_profiles.append(
            {"id": profile.get("id"), **frontend_profile.model_dump(exclude_none=False)}
        )

    return frontend_profiles


# ✅ 프로필 추가 - 필드명 변환 적용
@router.post(
    "/profile",
    response_model=UserProfileWithId,
    status_code=status.HTTP_201_CREATED,
    summary="새 프로필 추가",
)
async def add_new_profile(
    profile_data: UserProfile,
    current_user: dict = Depends(get_current_active_user),
):
    """새 프로필 추가 (필드명 변환 적용)"""
    user_uuid = current_user.get("id")

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    # ✅ 프론트엔드 필드명 → DB 필드명 변환
    db_data = profile_data.to_db_dict()

    ok, new_profile_id = db_ops.add_profile(user_uuid, db_data)
    if not ok:
        raise HTTPException(status_code=500, detail="프로필 추가에 실패했습니다.")

    return UserProfileWithId(id=new_profile_id, **profile_data.model_dump())


# ✅ 프로필 삭제 11.18수정
@router.delete(
    "/profile/{profile_id}",
    response_model=SuccessResponse,
    summary="특정 프로필 삭제",
)
async def delete_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    """특정 프로필 삭제"""
    user_uuid = current_user.get("id")

    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    ok, message = db_ops.delete_profile_by_id(profile_id, user_uuid)

    if not ok:
        raise HTTPException(status_code=500, detail=message)

    return SuccessResponse(message=message)


# 11.18 추가
# 비밀번호 변경 - 모든 기기에서 로그아웃 처리 (리프레시 토큰 무효화)
@router.put("/password", response_model=SuccessResponse, summary="비밀번호 변경")
async def change_password(
    request: PasswordChangeRequest,
    current_user: dict = Depends(get_current_active_user),
):
    """
    현재 사용자의 비밀번호를 변경하고, 모든 기기에서 로그아웃 처리합니다.
    (모든 리프레시 토큰 무효화)
    """
    user_uuid = current_user.get("id")
    username = current_user.get("userId")

    if not user_uuid or not username:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    # 1. 현재 비밀번호 확인
    stored_hash = db_ops.get_user_password_hash(username)
    if not stored_hash or not pwd_context.verify(request.current_password, stored_hash):
        raise HTTPException(
            status_code=400, detail="현재 비밀번호가 일치하지 않습니다."
        )

    # 2. 새 비밀번호 해시화 및 DB 업데이트
    new_password_hash = pwd_context.hash(request.new_password)
    ok, message = db_ops.update_user_password(user_uuid, new_password_hash)

    if not ok:
        raise HTTPException(status_code=500, detail=message)

    return SuccessResponse(
        message="비밀번호가 성공적으로 변경되었습니다. 보안을 위해 다시 로그인해주세요."
    )


# 11.18 추가: 액세스 토큰 재발급 엔드포인트
@router.post("/refresh", response_model=Token, summary="액세스 토큰 재발급")
async def refresh_access_token(request: RefreshTokenRequest):
    """리프레시 토큰으로 새 액세스 토큰을 발급합니다."""
    try:
        from app.auth import SECRET_KEY, ALGORITHM
        from jose import jwt, JWTError

        payload = jwt.decode(request.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        token_type: str = payload.get("type")

        if username is None or token_type != "refresh":
            raise HTTPException(
                status_code=401, detail="유효하지 않은 리프레시 토큰입니다."
            )

        user_uuid = db_ops.get_user_uuid_by_username(username)
        if not user_uuid:
            raise HTTPException(status_code=401, detail="사용자를 찾을 수 없습니다.")

        new_access_token = create_access_token(data={"sub": username})

        return Token(access_token=new_access_token, token_type="bearer")

    except JWTError:
        raise HTTPException(
            status_code=401, detail="리프레시 토큰이 만료되었거나 유효하지 않습니다."
        )


@router.post("/logout", response_model=SuccessResponse, summary="로그아웃")
async def logout_user():
    """
    로그아웃 처리. JWT는 stateless이므로 서버 측 처리 없이
    클라이언트에서 토큰을 삭제하는 것으로 로그아웃됩니다.
    """
    return SuccessResponse(message="성공적으로 로그아웃되었습니다.")


# ✅ 메인 프로필 설정 11.18 수정
@router.put(
    "/profile/main/{profile_id}",
    response_model=SuccessResponse,
    summary="메인 프로필 변경",
)
async def set_main_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    """메인 프로필 변경"""
    user_uuid = current_user.get("id")

    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    ok, message = db_ops.update_user_main_profile_id(user_uuid, profile_id)

    if not ok:
        raise HTTPException(status_code=500, detail=message)

    return SuccessResponse(message=message)


# ✅ 회원 탈퇴 11.18 수정
@router.delete(
    "/delete",
    response_model=SuccessResponse,
    summary="회원 탈퇴",
)
async def delete_user_account(
    current_user: dict = Depends(get_current_active_user),
):
    """회원 탈퇴 (모든 프로필 및 데이터 삭제)"""
    user_uuid = current_user.get("id")

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, message = db_ops.delete_user_account(user_uuid)

    if not ok:
        raise HTTPException(status_code=500, detail=message)

    return SuccessResponse(message=message)
