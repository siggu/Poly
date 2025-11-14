from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Any
from passlib.context import CryptContext

# DB 및 인증 관련 모듈을 가져옵니다.
from app.db.database import get_db
from app.auth import create_access_token, get_current_user

# DB 함수 및 스키마를 가져옵니다.
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

# -----------------------------------------------
# 새로운 의존성 함수: TokenData를 받아 DB에서 User 객체를 조회
# -----------------------------------------------


def get_current_active_user(
    token_data: TokenData = Depends(get_current_user),  # JWT에서 email 추출
) -> dict:
    """
    유효한 토큰으로부터 DB에서 현재 활성화된 사용자 객체(dict)를 조회합니다.
    """
    if token_data.username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="토큰에 사용자 정보가 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_uuid = db_ops.get_user_uuid_by_username(token_data.email)
    if user_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자를 찾을 수 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    ok, user_info = db_ops.get_user_and_profile_by_id(user_uuid)
    if not ok or user_info is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="사용자 정보를 가져올 수 없습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # is_active와 같은 필드가 있다면 여기서 확인 가능
    # if not user_info.get("isActive"):
    #     raise HTTPException(status_code=400, detail="비활성화된 계정입니다.")

    return user_info


# -----------------
# 인증 엔드포인트
# -----------------


@router.post(
    "/register",
    response_model=SuccessResponse,
    status_code=status.HTTP_201_CREATED,
    summary="사용자 등록",
)
async def register_user(user_data: UserCreate, db: Any = Depends(get_db)):
    """
    새로운 사용자를 등록합니다.
    요청 본문에는 다음 필드가 포함되어야 합니다:
    - **password**: 사용자 비밀번호
    - **username**: 사용자 이름 (프로필 이름)
    """
    # 이미 존재하는 사용자 확인
    if db_ops.check_user_exists(user_data.username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 존재하는 아이디입니다.",
        )

    hashed_password = pwd_context.hash(user_data.password)

    # 회원가입 시 프로필 데이터도 함께 생성
    full_user_data = {
        "username": user_data.username,  # email 필드 → DB username 컬럼
        "password_hash": hashed_password,  # 해시된 비밀번호
        "name": user_data.username,  # username 필��� → 프로필 이름
        "gender": "M",  # 기본값
        "birthDate": None,
        "location": None,
        "healthInsurance": None,
        "incomeLevel": 0.0,
        "basicLivelihood": "NONE",
        "disabilityLevel": "0",
        "longTermCare": "NONE",
        "pregnancyStatus": "없음",
    }

    ok, message = db_ops.create_user_and_profile(full_user_data)

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    return SuccessResponse(message=message)


@router.post("/login", response_model=Token, summary="사용자 로그인")
async def login_user(user_data: UserLogin, db: Any = Depends(get_db)):
    """
    사용자 인증을 처리하고, 성공 시 JWT Access Token을 반환합니다.
    """
    # DB에서 사용자 정보 조회
    stored_hash = db_ops.get_user_password_hash(user_data.username)

    if not stored_hash or not pwd_context.verify(user_data.password, stored_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="잘못된 아이디 또는 비밀번호입니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # JWT 토큰에 username을 저장
    access_token = create_access_token(data={"sub": user_data.username})
    return Token(access_token=access_token, token_type="bearer")


# -----------------
# 프로필 엔드포인트
# -----------------


@router.get("/profile", response_model=User, summary="현재 사용자의 메인 프로필 조회")
async def get_user_profile(current_user: dict = Depends(get_current_active_user)):
    """
    인증된 사용자의 메인 프로필 정보를 조회합니다.
    """
    # get_current_active_user가 반환하는 dict를 User 스키마에 맞게 변환
    return User(
        id=current_user.get("main_profile_id"),
        username=current_user.get("username"),
        created_at=current_user.get("created_at"),
        updated_at=current_user.get("updated_at"),
        profile=UserProfile(**current_user),
    )


@router.patch(
    "/profile/{profile_id}", response_model=SuccessResponse, summary="특정 프로필 수정"
)
async def update_user_profile(
    profile_id: int,
    update_data: UserProfile,
    current_user: dict = Depends(get_current_active_user),
):
    """
    인증된 사용자의 특정 프로필 정보를 수정합니다.
    """
    if not profile_id:
        raise HTTPException(status_code=404, detail="메인 프로필을 찾을 수 없습니다.")

    # `None` 값을 제외하고 업데이트할 데이터만 추출
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
    """
    인증된 사용자의 모든 프로필 목록을 조회합니다.
    """
    user_uuid = current_user.get("user_uuid")
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
    """
    인증된 사용자에게 새 프로필을 추가합니다.
    """
    user_uuid = current_user.get("user_uuid")
    ok, new_profile_id = db_ops.add_profile(user_uuid, profile_data.model_dump())
    if not ok:
        raise HTTPException(status_code=500, detail="프로필 추가에 실패했습니다.")

    # 추가된 프로필 정보를 다시 조회하여 반환
    # 이 부분은 간단하게 입력받은 데이터에 id만 추가하여 반환할 수도 있습니다.
    return UserProfileWithId(id=new_profile_id, **profile_data.model_dump())


@router.delete(
    "/profile/{profile_id}", response_model=SuccessResponse, summary="특정 프로필 삭제"
)
async def delete_user_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_active_user),
):
    """
    인증된 사용자의 특정 프로필을 삭제합니다.
    """
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
    """사용자의 메인 프로필을 변경합니다."""
    user_uuid = current_user.get("user_uuid")
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
    """
    인증된 사용자의 계정을 삭제합니다.
    """
    user_uuid = current_user.get("user_uuid")
    ok, message = db_ops.delete_user_account(user_uuid)
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return SuccessResponse(message=message)
