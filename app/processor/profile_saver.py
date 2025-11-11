# profile.py
# 목적: "Profile Saver" 노드
# - 사용자 입력 텍스트에서 '프로필 9개 항목'만 LLM으로 추출/정규화
# - DB(core_profile)에 멱등 업서트
# - LangGraph 상태에 저장 결과 기록
#
# 의존: pip install openai psycopg python-dotenv pydantic
#  - OPENAI_API_KEY 필수
#  - DB 접속: DATABASE_URL=postgresql+psycopg://user:pass@host:port/dbname
#
# 포함 필드(9개만):
#  1) birth_date (DATE, YYYY-MM-DD) 또는 나이 단서 → 생년월일이 명시된 경우만 저장
#  2) sex (M/F/O/N)  [선택: 사용자가 요구 시만]
#  3) residency_sgg_code (TEXT; 시군구코드 또는 "서울시 성동구" 같은 문자열)
#  4) insurance_type (EMPLOYED/LOCAL/DEPENDENT/MEDICAL_AID_1/MEDICAL_AID_2)
#  5) median_income_ratio (NUMERIC %)  ex) 48.5
#  6) basic_benefit_type (NONE/LIVELIHOOD/MEDICAL/HOUSING/EDUCATION)
#  7) disability_grade (SMALLINT: 0=미등록,1=심한,2=심하지않음)
#  8) ltci_grade (NONE/G1/G2/G3/G4/G5/COGNITIVE)
#  9) pregnant_or_postpartum12m (BOOLEAN)
#
# 정책:
# - 입력에 해당 필드가 "명시적/확정적"일 때만 업데이트. 모호/추정은 null 처리.
# - null은 기존 값을 덮어쓰지 않음(upsert에서 COALESCE 사용).
# - birth_date는 날짜 문자열이 있을 때만 저장(단순 "68세"는 미저장).
# - disability_grade는 0/1/2만 허용.
# - 성별/거주코드 등은 서비스 정책에 따라 제외 가능. (필드는 유지하되 값 미제공 허용)

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, TypedDict

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator
import psycopg

# ─────────────────────────────────────────────────────────────────────────────
# 0) 환경
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()
OPENAI_MODEL = os.getenv("PROFILE_EXTRACT_MODEL", "gpt-4o-mini")
OPENAI_JSON_HINT = {"type": "json_object"}

DB_URL = os.getenv("DATABASE_URL")  # e.g., postgresql+psycopg://user:pass@host:5432/db
if DB_URL and DB_URL.startswith("postgresql+psycopg://"):
    # psycopg.connect()는 postgresql:// 스킴을 기대. 접두어 정리.
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

client = OpenAI()


# ─────────────────────────────────────────────────────────────────────────────
# 1) LLM 시스템 프롬프트 (프로필 9개만 추출)
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """당신은 의료복지 '프로필 추출기'이다.
사용자 입력에서 다음 9개 항목만 추출하여 JSON으로 엄격히 출력하라.
모호하거나 추정이면 null로 둔다. 불필요한 키를 추가하지 않는다.

필드 정의:
- birth_date: 문자열(YYYY-MM-DD). 생년월일이 명시된 경우에만. 단순 '68세'는 null.
- sex: 'M'|'F'|'O'|'N' (O=Other, N=PreferNotToSay). 명시된 경우에만.
- residency_sgg_code: 시/도+시/군/구 명칭 또는 코드. 명시된 경우에만.
- insurance_type: 'EMPLOYED'|'LOCAL'|'DEPENDENT'|'MEDICAL_AID_1'|'MEDICAL_AID_2'.
- median_income_ratio: 숫자(0~300) 소수 가능. '%' 기호는 제거.
- basic_benefit_type: 'NONE'|'LIVELIHOOD'|'MEDICAL'|'HOUSING'|'EDUCATION'.
- disability_grade: 정수 0|1|2 (0=미등록, 1=심한, 2=심하지 않음).
- ltci_grade: 'NONE'|'G1'|'G2'|'G3'|'G4'|'G5'|'COGNITIVE'.
- pregnant_or_postpartum12m: 불리언. 임신 중이거나 출산 12개월 이내면 true, 아니면 false. 언급 없으면 null.

항상 아래 JSON 스키마로만 답하라:
{
  "birth_date": "YYYY-MM-DD" | null,
  "sex": "M"|"F"|"O"|"N" | null,
  "residency_sgg_code": string | null,
  "insurance_type": "EMPLOYED"|"LOCAL"|"DEPENDENT"|"MEDICAL_AID_1"|"MEDICAL_AID_2" | null,
  "median_income_ratio": number | null,
  "basic_benefit_type": "NONE"|"LIVELIHOOD"|"MEDICAL"|"HOUSING"|"EDUCATION" | null,
  "disability_grade": 0|1|2 | null,
  "ltci_grade": "NONE"|"G1"|"G2"|"G3"|"G4"|"G5"|"COGNITIVE" | null,
  "pregnant_or_postpartum12m": true|false | null
}
"""

USER_PROMPT_TEMPLATE = """사용자 입력:
{input_text}

위 기준에 따라 JSON으로만 답하라.
"""


# ─────────────────────────────────────────────────────────────────────────────
# 2) 출력 스키마(검증)
# ─────────────────────────────────────────────────────────────────────────────
class ProfilePayload(BaseModel):
    birth_date: Optional[str] = Field(None)
    sex: Optional[str] = Field(None)
    residency_sgg_code: Optional[str] = Field(None)
    insurance_type: Optional[str] = Field(None)
    median_income_ratio: Optional[float] = Field(None)
    basic_benefit_type: Optional[str] = Field(None)
    disability_grade: Optional[int] = Field(None)
    ltci_grade: Optional[str] = Field(None)
    pregnant_or_postpartum12m: Optional[bool] = Field(None)

    @field_validator("birth_date")
    @classmethod
    def _v_birth(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return None
        return v

    @field_validator("sex")
    @classmethod
    def _v_sex(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        m = v.upper()
        return m if m in {"M", "F", "O", "N"} else None

    @field_validator("insurance_type")
    @classmethod
    def _v_ins(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        m = v.upper()
        allowed = {"EMPLOYED", "LOCAL", "DEPENDENT", "MEDICAL_AID_1", "MEDICAL_AID_2"}
        return m if m in allowed else None

    @field_validator("basic_benefit_type")
    @classmethod
    def _v_basic(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        m = v.upper()
        allowed = {"NONE", "LIVELIHOOD", "MEDICAL", "HOUSING", "EDUCATION"}
        return m if m in allowed else None

    @field_validator("disability_grade")
    @classmethod
    def _v_disab(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return v
        return v if v in (0, 1, 2) else None

    @field_validator("ltci_grade")
    @classmethod
    def _v_ltci(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        m = v.upper()
        allowed = {"NONE", "G1", "G2", "G3", "G4", "G5", "COGNITIVE"}
        return m if m in allowed else None

    @field_validator("median_income_ratio")
    @classmethod
    def _v_mir(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        try:
            f = float(v)
        except Exception:
            return None
        return f if 0.0 <= f <= 300.0 else None


# ─────────────────────────────────────────────────────────────────────────────
# 3) LLM 호출 (JSON 강제)
# ─────────────────────────────────────────────────────────────────────────────
def extract_profile_fields(input_text: str) -> ProfilePayload:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT_TEMPLATE.format(input_text=input_text)}
    ]
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        response_format=OPENAI_JSON_HINT,
        temperature=0
    )
    raw = (resp.choices[0].message.content or "").strip()
    data = json.loads(raw)
    return ProfilePayload(**data)


# ─────────────────────────────────────────────────────────────────────────────
# 4) 업서트 (명시된 값만 갱신; null은 유지)
# ─────────────────────────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO core_profile AS cp (
  user_id, birth_date, residency_sgg_code, insurance_type, median_income_ratio,
  basic_benefit_type, disability_grade, ltci_grade, pregnant_or_postpartum12m, updated_at
) VALUES (
  %(user_id)s, %(birth_date)s, %(residency_sgg_code)s, %(insurance_type)s, %(median_income_ratio)s,
  %(basic_benefit_type)s, %(disability_grade)s, %(ltci_grade)s, %(pregnant_or_postpartum12m)s, now()
)
ON CONFLICT (user_id) DO UPDATE SET
  birth_date = COALESCE(EXCLUDED.birth_date, cp.birth_date),
  residency_sgg_code = COALESCE(EXCLUDED.residency_sgg_code, cp.residency_sgg_code),
  insurance_type = COALESCE(EXCLUDED.insurance_type, cp.insurance_type),
  median_income_ratio = COALESCE(EXCLUDED.median_income_ratio, cp.median_income_ratio),
  basic_benefit_type = COALESCE(EXCLUDED.basic_benefit_type, cp.basic_benefit_type),
  disability_grade = COALESCE(EXCLUDED.disability_grade, cp.disability_grade),
  ltci_grade = COALESCE(EXCLUDED.ltci_grade, cp.ltci_grade),
  pregnant_or_postpartum12m = COALESCE(EXCLUDED.pregnant_or_postpartum12m, cp.pregnant_or_postpartum12m),
  updated_at = now();
"""

def upsert_profile(user_id: str, payload: ProfilePayload) -> Dict[str, Any]:
    """
    null 값은 기존 값 유지. (COALESCE)
    반환: {"updated": True/False, "applied": {필드:값...}}
    """
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")

    # psycopg는 None을 SQL NULL로 처리
    params = {
        "user_id": user_id,
        "birth_date": payload.birth_date,
        "residency_sgg_code": payload.residency_sgg_code,
        "insurance_type": payload.insurance_type,
        "median_income_ratio": payload.median_income_ratio,
        "basic_benefit_type": payload.basic_benefit_type,
        "disability_grade": payload.disability_grade,
        "ltci_grade": payload.ltci_grade,
        "pregnant_or_postpartum12m": payload.pregnant_or_postpartum12m,
    }

    # 업데이트가 실제 발생했는지 판단하기 위해, upsert 전후 row를 비교하는 방법이 가장 정확하나
    # 단순화를 위해 여기서는 실행 성공 시 updated=True로 둔다.
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)

    applied = {k: v for k, v in params.items() if k != "user_id" and v is not None}
    return {"updated": True if applied else False, "applied": applied}


# ─────────────────────────────────────────────────────────────────────────────
# 5) LangGraph 노드 인터페이스
# ─────────────────────────────────────────────────────────────────────────────
class State(TypedDict, total=False):
    user_id: str
    input_text: str
    router: Dict[str, Any]
    profile_delta: Dict[str, Any]
    persist_result: Dict[str, Any]

def profile_saver_node(state: State) -> State:
    """
    사전 조건:
      - router.target in {'PROFILE','BOTH'} 일 때만 호출
    동작:
      - LLM으로 9개 필드 JSON 추출 → 검증/정규화 → DB upsert → state 갱신
    """
    user_id = state.get("user_id") or ""
    text = (state.get("input_text") or "").strip()
    if not user_id or not text:
        state["profile_delta"] = {"error": "missing user_id or input_text"}
        return state

    try:
        payload = extract_profile_fields(text)
    except Exception as e:
        state["profile_delta"] = {"error": f"extract_failed: {type(e).__name__}", "detail": str(e)}
        return state

    try:
        result = upsert_profile(user_id, payload)
    except Exception as e:
        state["profile_delta"] = {"error": f"upsert_failed: {type(e).__name__}", "detail": str(e)}
        return state

    state["profile_delta"] = payload.model_dump()
    pr = state.get("persist_result") or {}
    pr.update({"profile_upserted": bool(result.get("updated")), "profile_applied": result.get("applied")})
    state["persist_result"] = pr
    return state


# ─────────────────────────────────────────────────────────────────────────────
# 6) 단독 실행 테스트
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 예시 입력: 나이만 언급 → birth_date는 null로 추출되어 저장되지 않음(정책)
    example_inputs = [
        "저 68세고 의료급여 2종입니다. 중위소득은 45% 정도예요.",
        "저는 서울시 성동구에 살고 있고, 장기요양 2등급입니다. 장애는 심하지 않음.",
        "출산한 지 6개월 됐고, 기초생활보장 의료급여 받고 있어요.",
        "1990-05-10 생이고, 직장가입자입니다.",
    ]

    uid = os.getenv("TEST_USER_ID", "u_demo_1")
    for t in example_inputs:
        s: State = {"user_id": uid, "input_text": t}
        out = profile_saver_node(s)
        print(json.dumps(out.get("profile_delta", {}), ensure_ascii=False))
        print(json.dumps(out.get("persist_result", {}), ensure_ascii=False))
        print("-" * 60)
