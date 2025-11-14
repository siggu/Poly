# app/langgraph/nodes/info_extractor.py
# -*- coding: utf-8 -*-
"""
info_extractor.py

역할:
  - 현재 user_input에서 의료·복지 정책 추천에 중요한 정보만 추출해
    1) ephemeral_profile (프로필 오버레이)
    2) ephemeral_collection (사례/병력 트리플)
    에 넣어준다.

동작 규칙:
  - query_router가 남긴 state["router"]를 먼저 본다.
    - save_profile == False 이면 프로필 추출은 하지 않음
    - save_collection == False 이면 트리플 추출은 하지 않음
  - 둘 다 False 이면:
    → 아무것도 추출하지 않고 그대로 통과 (no-op)
  - 그 외:
    → LLM을 호출해서 구조화된 JSON을 받은 뒤
       ephemeral_profile / ephemeral_collection 에 병합

ephemeral_profile 구조 예:
  {
    "age": {"value": 68, "confidence": 0.95},
    "region_gu": {"value": "강북구", "confidence": 0.9},
    "income_median_ratio": {"value": 120, "confidence": 0.9},
    ...
  }

ephemeral_collection 구조 예:
  {
    "triples": [
      {
        "subject": "self",
        "predicate": "disease_code",
        "object": "당뇨병",
        "code_system": "KCD7",
        "code": "E11",
        "confidence": 0.95
      },
      ...
    ]
  }

주의:
  - 여기서는 DB에 직접 쓰지 않는다. (persist_pipeline에서 병합/업서트)
  - router.use_rag 여부는 여기서는 참고만 하고, 실제 RAG 여부는 retrieval_planner에서 처리.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict, Literal

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

# LangSmith trace 데코레이터 (없으면 no-op)
try:
    from langsmith import traceable
except Exception:  # pragma: no cover
    def traceable(func):
        return func

from app.langgraph.state.ephemeral_context import State, Message

load_dotenv()

INFO_EXTRACTOR_MODEL = os.getenv("INFO_EXTRACTOR_MODEL", os.getenv("ROUTER_MODEL", "gpt-4o-mini"))

_client: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()
    return _client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────
# Pydantic 스키마 (LLM 출력용)
# ─────────────────────────────────────────────────────────

class ProfileField(BaseModel):
    value: Optional[str] = Field(
        description="추출된 값. 숫자(나이, 퍼센트 등)도 문자열로 넣어도 됨. 모르면 null."
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="0~1 사이 신뢰도. 확실하면 0.9 이상, 애매하면 0.5 이하."
    )


class ExtractedProfile(BaseModel):
    """
    의료 복지 정책에 자주 쓰이는 핵심 프로필 필드들만 정의.
    """
    age: Optional[ProfileField] = None                      # 나이(만 나이 기준)
    birth_year: Optional[ProfileField] = None               # 출생 연도 (예: 1957)
    sex: Optional[ProfileField] = None                      # '남' / '여'
    region_gu: Optional[ProfileField] = None                # 거주 구 (예: '강북구', '동작구')
    income_median_ratio: Optional[ProfileField] = None      # 중위소득 대비 퍼센트 (예: '120')
    basic_benefit_type: Optional[ProfileField] = None       # 기초생활보장 급여 구분 (생계/의료/주거/교육/기타)
    nhis_qualification: Optional[ProfileField] = None       # 건강보험 자격 (직장/지역/피부양/의료급여 등)
    disability_grade: Optional[ProfileField] = None         # 장애 정도/등급 (예: '장애 2급', '중증', '경증')
    ltci_grade: Optional[ProfileField] = None               # 장기요양등급 (예: '장기요양 2등급')
    pregnancy_status: Optional[ProfileField] = None         # 임신/출산 상태 (예: '임신 30주차')


class Triple(BaseModel):
    subject: str = Field(
        description="보통 'self' (사용자 자신), 또는 'child', 'spouse' 등."
    )
    predicate: str = Field(
        description="관계/속성 이름. 예: 'disease', 'disease_code', 'hospitalization', 'surgery', 'treatment', 'episode'."
    )
    object: str = Field(
        description="자유 텍스트 값 (예: '당뇨병', '2024-06 유방암 진단 후 항암치료 중')."
    )
    code_system: Optional[str] = Field(
        default=None,
        description="코드 체계 (알면). 예: 'KCD7', 'ICD-10', 'NHIS'. 모르면 null."
    )
    code: Optional[str] = Field(
        default=None,
        description="코드 값 (알면). 예: 'E11', 'C509'. 모르면 null."
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="트리플의 신뢰도 (0~1)."
    )


class ExtractedCollection(BaseModel):
    triples: List[Triple] = Field(default_factory=list)


class ExtractResult(BaseModel):
    profile: ExtractedProfile
    collection: ExtractedCollection


class InfoExtractorOutput(TypedDict, total=False):
    ephemeral_profile: Dict[str, Any]
    ephemeral_collection: Dict[str, Any]
    messages: Any  # StateGraph reducer가 merge


SYSTEM_PROMPT = """
너는 의료·복지 상담 챗봇의 정보 추출기 역할을 한다.
사용자의 발화에서 복지 정책 추천에 유의미한 정보만 뽑아
지정된 JSON 스키마에 맞추어 응답하라.

- 프로필 정보(profile)는 '상태/속성' (나이, 거주지, 소득수준, 건강보험 자격, 기초생활보장 급여, 장애등급, 장기요양등급, 임신/출산 여부 등)
- 컬렉션 정보(collection)는 '사례/병력/에피소드' (언제 어떤 진단, 수술, 입원, 치료, 합병증, 투약, 검사 등을 받았는지)

주의:
- 정보가 명확하지 않으면 value를 null로 두고 confidence를 0.0~0.5 정도로 낮게 설정해도 된다.
- 모르는 필드는 그냥 null로 둔다.
- 숫자(나이, 퍼센트 등)는 문자열로 넣어도 괜찮다.
- 가능한 경우 질병명에 대응하는 코드(KCD7/ICD-10 등)를 추정해서 code_system, code에 넣어도 되지만, 자신 없으면 null로 두어라.

반드시 아래 형태의 JSON만 응답하라:

{
  "profile": {
    "age": {"value": "...", "confidence": 0.9} | null,
    "birth_year": {...} | null,
    "sex": {...} | null,
    "region_gu": {...} | null,
    "income_median_ratio": {...} | null,
    "basic_benefit_type": {...} | null,
    "nhis_qualification": {...} | null,
    "disability_grade": {...} | null,
    "ltci_grade": {...} | null,
    "pregnancy_status": {...} | null
  },
  "collection": {
    "triples": [
      {
        "subject": "self" | "child" | "spouse" | ...,
        "predicate": "disease" | "disease_code" | "hospitalization" | "surgery" | "treatment" | "episode" | ...,
        "object": "...",
        "code_system": "KCD7" | "ICD-10" | "NHIS" | null,
        "code": "E11" | "C509" | ... | null,
        "confidence": 0.0~1.0
      },
      ...
    ]
  }
}
""".strip()


def _call_info_llm(text: str) -> ExtractResult:
    client = _get_client()
    resp = client.chat.completions.create(
        model=INFO_EXTRACTOR_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        temperature=0.0,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content or ""
    data = json.loads(raw)
    try:
        result = ExtractResult(**data)
    except ValidationError as e:
        raise ValueError(f"InfoExtractor validation failed: {e}\nGot: {data}")
    return result


def _merge_ephemeral_profile(
    old: Dict[str, Any],
    extracted: ExtractedProfile,
    save_profile: bool,
) -> Dict[str, Any]:
    """
    기존 ephemeral_profile + 새 추출 결과 병합.
    - save_profile=False이면 그냥 old 그대로 반환.
    - 값이 있는 필드만 {value, confidence} 형태로 채워 넣는다.
    """
    if not save_profile:
        return dict(old or {})

    result = dict(old or {})

    def set_field(name: str, field: Optional[ProfileField]):
        if field is None or field.value in (None, ""):
            return
        result[name] = {"value": field.value, "confidence": field.confidence}

    set_field("age", extracted.age)
    set_field("birth_year", extracted.birth_year)
    set_field("sex", extracted.sex)
    set_field("region_gu", extracted.region_gu)
    set_field("income_median_ratio", extracted.income_median_ratio)
    set_field("basic_benefit_type", extracted.basic_benefit_type)
    set_field("nhis_qualification", extracted.nhis_qualification)
    set_field("disability_grade", extracted.disability_grade)
    set_field("ltci_grade", extracted.ltci_grade)
    set_field("pregnancy_status", extracted.pregnancy_status)

    return result


def _merge_ephemeral_collection(
    old: Any,
    extracted: ExtractedCollection,
    save_collection: bool,
) -> Dict[str, Any]:
    """
    기존 ephemeral_collection + 새 추출 결과 병합.
    - save_collection=False이면 old 그대로 반환 (dict로 감싸줌).
    - triples 리스트를 단순 append (중복 제거는 persist_pipeline에서 담당 가능).
    """
    if isinstance(old, dict):
        existing_triples = list(old.get("triples") or [])
    elif isinstance(old, list):
        existing_triples = list(old)
    else:
        existing_triples = []

    if not save_collection:
        # 기존 구조를 유지만 해준다.
        return {"triples": existing_triples}

    new_triples = []
    for t in extracted.triples:
        new_triples.append({
            "subject": t.subject,
            "predicate": t.predicate,
            "object": t.object,
            "code_system": t.code_system,
            "code": t.code,
            "confidence": t.confidence,
        })

    merged = existing_triples + new_triples
    return {"triples": merged}


@traceable
def extract(state: State) -> InfoExtractorOutput:
    """
    LangGraph 노드 함수.

    입력:
      - state["user_input"]: 현재 턴 사용자 발화
      - state["router"]: query_router가 판단한 Decision dict
      - state["ephemeral_profile"]: 기존 임시 프로필
      - state["ephemeral_collection"]: 기존 임시 컬렉션(트리플들)

    출력:
      - ephemeral_profile: 병합된 프로필 오버레이
      - ephemeral_collection: 병합된 트리플 리스트
      - messages: tool 로그 append
    """
    text = (state.get("user_input") or "").strip()
    msgs: List[Message] = list(state.get("messages") or [])

    router_info = state.get("router") or {}
    save_profile = bool(router_info.get("save_profile"))
    save_collection = bool(router_info.get("save_collection"))

    # 아무것도 추출할 필요가 없으면 no-op
    if not save_profile and not save_collection:
        msgs.append({
            "role": "tool",
            "content": "[info_extractor] skip (save_profile=False, save_collection=False)",
            "created_at": _now_iso(),
            "meta": {"router": router_info},
        })
        return {
            "ephemeral_profile": dict(state.get("ephemeral_profile") or {}),
            "ephemeral_collection": dict(state.get("ephemeral_collection") or {"triples": []}),
            "messages": msgs,
        }

    if not text:
        msgs.append({
            "role": "tool",
            "content": "[info_extractor] empty user_input; nothing extracted",
            "created_at": _now_iso(),
            "meta": {"router": router_info},
        })
        return {
            "ephemeral_profile": dict(state.get("ephemeral_profile") or {}),
            "ephemeral_collection": dict(state.get("ephemeral_collection") or {"triples": []}),
            "messages": msgs,
        }

    try:
        result = _call_info_llm(text)
        old_profile = dict(state.get("ephemeral_profile") or {})
        old_collection = state.get("ephemeral_collection") or {"triples": []}

        merged_profile = _merge_ephemeral_profile(old_profile, result.profile, save_profile)
        merged_collection = _merge_ephemeral_collection(old_collection, result.collection, save_collection)

        # 로그
        n_profile_fields = sum(1 for v in merged_profile.values() if isinstance(v, dict) and "value" in v)
        n_triples = len(merged_collection.get("triples") or [])

        msgs.append({
            "role": "tool",
            "content": "[info_extractor] extracted profile/collection",
            "created_at": _now_iso(),
            "meta": {
                "router": router_info,
                "profile_fields": n_profile_fields,
                "triples_total": n_triples,
            },
        })

        return {
            "ephemeral_profile": merged_profile,
            "ephemeral_collection": merged_collection,
            "messages": msgs,
        }

    except Exception as e:
        # 실패 시 안전 폴백: 아무것도 바꾸지 않고 로그만 남김
        msgs.append({
            "role": "tool",
            "content": "[info_extractor] error; keep previous state",
            "created_at": _now_iso(),
            "meta": {"error": str(e), "router": router_info},
        })
        return {
            "ephemeral_profile": dict(state.get("ephemeral_profile") or {}),
            "ephemeral_collection": dict(state.get("ephemeral_collection") or {"triples": []}),
            "messages": msgs,
        }


if __name__ == "__main__":
    # 단독 테스트용 (직접 실행 시)
    dummy_state: State = {  # type: ignore
        "user_input": "저는 68세 강북구 사는 의료급여 2종이고, 당뇨랑 고혈압이 있어요.",
        "messages": [],
        "ephemeral_profile": {},
        "ephemeral_collection": {},
        "router": {
            "category": "PROFILE_UPDATE",
            "save_profile": True,
            "save_collection": True,
            "use_rag": False,
            "reason": "테스트",
        },
    }
    out = extract(dummy_state)  # type: ignore
    print("ephemeral_profile:", json.dumps(out["ephemeral_profile"], ensure_ascii=False, indent=2))
    print("ephemeral_collection:", json.dumps(out["ephemeral_collection"], ensure_ascii=False, indent=2))
