"""User & Auth 관련 API 엔드포인트 - 최종 수정 버전"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Any
from passlib.context import CryptContext

from app.db.database import get_db
from app.auth import create_access_token, get_current_user
from app.db import database as db_ops
from app.schemas import (
    UserCreate,
    UserLogin,
    UserProfile,
    UserProfileWithId,
    Token,
    TokenData,
    SuccessResponse,
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
    if user_info and 'user_uuid' in user_info:
        user_info['id'] = user_info['user_uuid']
    return user_info


# ===============================================
# 인증 엔드포인트
# ===============================================


@router.post(
    "/register", response_model=SuccessResponse, status_code=status.HTTP_201_CREATED
)
async def register_user(user_data: UserCreate, db: Any = Depends(get_db)):
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
async def login_user(user_data: UserLogin, db: Any = Depends(get_db)):
    """로그인 및 JWT 토큰 발급"""
    stored_hash = db_ops.get_user_password_hash(user_data.username)

    if not stored_hash or not pwd_context.verify(user_data.password, stored_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="잘못된 아이디 또는 비밀번호입니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user_data.username})
    return Token(access_token=access_token, token_type="bearer")


# ===============================================
# 프로필 엔드포인트
# ===============================================


@router.get("/profile", summary="현재 사용자의 메인 프로필 조회")
async def get_user_profile(current_user: dict = Depends(get_current_active_user)):
    """
    인증된 사용자의 메인 프로필 정보를 조회합니다.
    ✅ main_profile_id를 포함하여 반환합니다.
    """
    # get_current_active_user가 반환하는 딕셔너리를 그대로 반환합니다.
    # 이 딕셔너리는 이미 필요한 모든 정보를 포함하고 있습니다.
    return current_user


@router.patch(
    "/profile/{profile_id}", response_model=SuccessResponse, summary="특정 프로필 수정"
)
async def update_user_profile(
    profile_id: int,
    update_data: UserProfile,
    current_user: dict = Depends(get_current_active_user),
):
    """특정 프로필 정보 수정"""
    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    update_dict = update_data.model_dump(exclude_unset=True)

    if not update_dict:
        return SuccessResponse(message="수정할 내용이 없습니다.")

    if db_ops.update_profile(profile_id, update_dict):
        return SuccessResponse(message="프로필이 성공적으로 수정되었습니다.")
    else:
        raise HTTPException(status_code=500, detail="프로필 수정에 실패했습니다.")


@router.get(
    "/profiles", response_model=List[UserProfileWithId], summary="모든 프로필 조회"
)
async def get_all_user_profiles(current_user: dict = Depends(get_current_active_user)):
    """인증된 사용자의 모든 프로필 목록 조회"""
    user_uuid = current_user.get("id")  # ✅ "user_uuid" → "id"

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, profiles = db_ops.get_all_profiles_by_user_id(user_uuid)
    if not ok:
        raise HTTPException(
            status_code=500, detail="프로필 목록을 가져오는 데 실패했습니다."
        )
    return profiles


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
    """새 프로필 추가"""
    user_uuid = current_user.get("id")  # ✅ "user_uuid" → "id"

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, new_profile_id = db_ops.add_profile(user_uuid, profile_data.model_dump())
    if not ok:
        raise HTTPException(status_code=500, detail="프로필 추가에 실패했습니다.")

    return UserProfileWithId(id=new_profile_id, **profile_data.model_dump())


@router.delete(
    "/profile/{profile_id}", response_model=SuccessResponse, summary="특정 프로필 삭제"
)
async def delete_user_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    """특정 프로필 삭제"""
    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    if db_ops.delete_profile_by_id(profile_id):
        return SuccessResponse(message="프로필이 성공적으로 삭제되었습니다.")
    else:
        raise HTTPException(status_code=500, detail="프로필 삭제에 실패했습니다.")


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
    if not profile_id:
        raise HTTPException(status_code=400, detail="유효하지 않은 프로필 ID입니다.")

    user_uuid = current_user.get("id")  # ✅ "user_uuid" → "id"

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, msg = db_ops.update_user_main_profile_id(user_uuid, profile_id)
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return SuccessResponse(message=msg)


@router.delete(
    "/delete", response_model=SuccessResponse, summary="현재 사용자 계정 삭제"
)
async def delete_user_account(
    current_user: dict = Depends(get_current_active_user),
):
    """사용자 계정 삭제"""
    user_uuid = current_user.get("id")  # ✅ "user_uuid" → "id"

    if not user_uuid:
        raise HTTPException(status_code=401, detail="사용자 정보를 찾을 수 없습니다.")

    ok, message = db_ops.delete_user_account(user_uuid)
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return SuccessResponse(message=message)
        