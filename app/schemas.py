"""Pydantic 스키마 정의 파일입니다.
사용자, 인증, 프로필 등 다양한 데이터 구조를 정의합니다. 11.14수정(컬럼수정)"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# ==============================================================================
# 인증 및 토큰 관련 스키마
# ==============================================================================


class TokenData(BaseModel):
    """
    JWT 토큰 내부에 포함될 데이터의 스키마를 정의합니다.

    Attributes:
        email (Optional[str]): 사용자의 username (아이디).
                                인증 시 토큰을 디코딩하여 이 정보를 사용합니다.
    """

    # 토큰에 username을 담을지 여부는 선택사항(Optional)으로 처리합니다.
    # 토큰 검증 과정에서 None이 될 수도 있기 때문입니다.
    username: Optional[str] = None  # DB의 username을 저장


class Token(BaseModel):
    """
    클라이언트에게 반환될 토큰 정보를 정의합니다.
    """

    access_token: str
    token_type: str = "bearer"


class SuccessResponse(BaseModel):
    """성공 메시지 반환"""

    message: str


# ==============================================================================
# 사용자 및 프로필 관련 스키마
# ==============================================================================


class UserBase(BaseModel):
    """사용자 기본 정보 구조"""

    username: Optional[str] = None


class UserCreate(UserBase):
    """회원가입 시 필요한 사용자 정보 구조"""

    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=8, max_length=72)
    name: str = Field(..., min_length=2, max_length=50)
    birth_date: Optional[str] = None
    sex: Optional[str] = None
    residency_sgg_code: Optional[str] = None
    insurance_type: Optional[str] = None
    median_income_ratio: Optional[float] = None
    basic_benefit_type: Optional[str] = None
    disability_grade: Optional[str] = None
    ltci_grade: Optional[str] = None
    pregnant_or_postpartum12m: Optional[str] = None


class UserLogin(BaseModel):
    """사용자 로그인 정보 구조"""

    username: str
    password: str


class UserProfile(BaseModel):
    """
    사용자 프로필 정보 구조
    ✅ 프론트엔드 필드명과 일치하도록 수정
    ✅ field_validator로 DB 필드명으로 자동 변환
    """

    # 프론트엔드 필드명 (camelCase)
    name: Optional[str] = None
    gender: Optional[str] = None  # ✅ sex → gender
    birthDate: Optional[str] = None
    location: Optional[str] = None  # ✅ residency_sgg_code → location
    healthInsurance: Optional[str] = None  # ✅ insurance_type → healthInsurance
    incomeLevel: Optional[float] = None  # ✅ median_income_ratio → incomeLevel
    basicLivelihood: Optional[str] = None  # ✅ basic_benefit_type → basicLivelihood
    disabilityLevel: Optional[str] = None  # ✅ disability_grade → disabilityLevel
    longTermCare: Optional[str] = None
    pregnancyStatus: Optional[str] = None
    isActive: Optional[bool] = None

    # ✅ DB로 저장하기 전에 필드명 변환
    def to_db_dict(self) -> dict:
        """프론트엔드 필드명을 DB 필드명으로 변환"""
        return {
            "name": self.name,
            "sex": self.gender,  # gender → sex
            "birth_date": self.birthDate,  # birthDate → birth_date
            "residency_sgg_code": self.location,  # location → residency_sgg_code
            "insurance_type": self.healthInsurance,  # healthInsurance → insurance_type
            "median_income_ratio": self.incomeLevel,  # incomeLevel → median_income_ratio
            "basic_benefit_type": self.basicLivelihood,  # basicLivelihood → basic_benefit_type
            "disability_grade": self.disabilityLevel,  # disabilityLevel → disability_grade
            "ltci_grade": self.longTermCare,  # longTermCare → ltci_grade
            "pregnant_or_postpartum12m": self.pregnancyStatus,  # pregnancyStatus → pregnant_or_postpartum12m
        }

    # ✅ DB에서 가져온 데이터를 프론트엔드 형식으로 변환
    @classmethod
    def from_db_dict(cls, db_data: dict):
        """DB 필드명을 프론트엔드 필드명으로 변환"""
        return cls(
            name=db_data.get("name"),
            gender=db_data.get("sex"),  # sex → gender
            birthDate=db_data.get("birth_date"),  # birth_date → birthDate
            location=db_data.get("residency_sgg_code"),  # residency_sgg_code → location
            healthInsurance=db_data.get(
                "insurance_type"
            ),  # insurance_type → healthInsurance
            incomeLevel=db_data.get(
                "median_income_ratio"
            ),  # median_income_ratio → incomeLevel
            basicLivelihood=db_data.get(
                "basic_benefit_type"
            ),  # basic_benefit_type → basicLivelihood
            disabilityLevel=db_data.get(
                "disability_grade"
            ),  # disability_grade → disabilityLevel
            longTermCare=db_data.get("ltci_grade"),  # ltci_grade → longTermCare
            pregnancyStatus=db_data.get(
                "pregnant_or_postpartum12m"
            ),  # pregnant_or_postpartum12m → pregnancyStatus
            isActive=db_data.get("is_active", False),
        )


class UserProfileWithId(UserProfile):
    """DB에서 조회 시 사용, 프로필 ID 포함"""

    id: int


class User(UserBase):
    """사용자 정보 구조, 프로필 포함"""

    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    profile: Optional[UserProfile] = None

    class Config:
        from_attributes = True


# --------------------------------------------------
# End of Pydantic 스키마 정의 파일
# --------------------------------------------------
