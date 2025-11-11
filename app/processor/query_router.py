# router_node.py (updated)
# 목적: Router LLM 노드 - 입력을 분석해 저장 대상/필요 RAG를 결정
# 변경 사항:
#  - Profile 판정 기준을 "나이/생년월일, 성별, 거주지(시군구), 건강보험 자격, 중위소득 대비 소득수준,
#    기초생활보장 급여 구분, 장애 등급(0/1/2), 장기요양 등급, 임신·출산 여부" 로 한정
#  - target 에 'BOTH' 추가 (혼합 입력 시)
# 의존: pip install openai langgraph pydantic python-dotenv

from __future__ import annotations
import json
import re
from typing import Literal, Optional, TypedDict
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv
from openai import OpenAI
from langsmith import traceable
from langsmith.wrappers import wrap_openai
# ─────────────────────────────────────────────────────────────────────────────
# 0) 설정
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()
client = OpenAI()

MODEL_NAME = "gpt-4o-mini"  # 필요시 변경
JSON_HINT = {"type": "json_object"}  # OpenAI SDK: response_format


# ─────────────────────────────────────────────────────────────────────────────
# 1) 출력 스키마(엄격)
# ─────────────────────────────────────────────────────────────────────────────
TargetT = Literal["PROFILE", "COLLECTION", "BOTH", "NONE"]
RagT = Literal["PROFILE", "COLLECTION", "BOTH", "NONE"]

class RouterDecision(BaseModel):
    target: TargetT = Field(..., description="PROFILE | COLLECTION | BOTH | NONE")
    required_rag: RagT = Field(..., description="PROFILE | COLLECTION | BOTH | NONE")
    reason: str = Field(..., min_length=1, max_length=400)

class RouterState(TypedDict, total=False):
    user_id: str
    input_text: str
    router: dict


# ─────────────────────────────────────────────────────────────────────────────
# 2) 시스템 프롬프트(정책) — Profile 기준 ‘9개 항목’으로 한정
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """당신은 의료복지 데이터 처리 시스템의 라우터이다.
사용자 입력을 분석해 JSON으로만 출력하라.

### 저장 대상 판단 규칙

- Profile로 저장해야 하는 경우(다음 9개 항목만 해당):
  1) 나이/생년월일
  2) 성별
  3) 거주지(시군구)
  4) 건강보험 자격(직장/지역/피부양/의료급여)
  5) 중위소득 대비 소득수준(%)
  6) 기초생활보장 급여 구분(없음/생계/의료/주거/교육)
  7) 장애 등급(0=미등록, 1=심한, 2=심하지 않음)
  8) 장기요양 등급(NONE/G1~G5/COGNITIVE)
  9) 임신·출산 여부(출산 12개월 포함)

- Collection(SPO)로 저장해야 하는 경우:
  질병/진단/코드, 증상/병기, 치료/수술/약물/재활/투석,
  산정특례, 난임/고위험 임신, 재난·실직·소득급감,
  문서(진단서/영수증/검진), 기간/시점 등 서술형·종류가 많은 정보.

- 입력에 Profile 정보와 Collection 정보가 함께 있으면 target='BOTH'.
  단, Profile 9개 항목에 **정확히 매핑 가능한 값**만 Profile로 간주한다.
  (예: “나이 68세”는 Profile 후보이나 생년월일이 불명확하면 보수적으로 판단 가능)

- 저장하지 않음(NONE):
  인사말/잡담, 개인 정보 없이 정책 일반 문의, 모델 테스트 등.

### RAG 필요 판단 규칙
- Profile 관련 질문 → PROFILE
- 의료/치료/특례/문서 → COLLECTION
- 자격 판단/혜택 추천 → BOTH
- 일반 정책·FAQ → NONE

### 출력 형식(JSON만)
{
  "target": "PROFILE" | "COLLECTION" | "BOTH" | "NONE",
  "required_rag": "PROFILE" | "COLLECTION" | "BOTH" | "NONE",
  "reason": "<간단한 한 문장>"
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# 3) LLM 호출 + 강건한 JSON 파싱
# ─────────────────────────────────────────────────────────────────────────────

def _extract_json(text: str) -> str:
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        raise ValueError("No JSON object found in LLM output")
    return m.group(0)

def call_router_llm(input_text: str) -> RouterDecision:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"사용자 입력:\n{input_text}\n\n위 기준에 따라 JSON으로만 답하라."}
    ]
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        response_format=JSON_HINT,
        temperature=0
    )
    raw = resp.choices[0].message.content.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = json.loads(_extract_json(raw))

    try:
        decision = RouterDecision(**data)
    except ValidationError as e:
        raise ValueError(f"RouterDecision validation failed: {e}\nGot: {data}")
    return decision


# ─────────────────────────────────────────────────────────────────────────────
# 4) LangGraph 노드 함수
# ─────────────────────────────────────────────────────────────────────────────
@traceable
def router_node(state: RouterState) -> RouterState:
    """
    입력: state["input_text"]
    출력: state["router"] = {target, required_rag, reason}
    오류 시: target='NONE', required_rag='NONE'로 폴백
    """
    text = (state.get("input_text") or "").strip()
    if not text:
        state["router"] = {
            "target": "NONE",
            "required_rag": "NONE",
            "reason": "빈 입력"
        }
        return state

    try:
        decision = call_router_llm(text)
        state["router"] = decision.model_dump()
    except Exception as e:
        state["router"] = {
            "target": "NONE",
            "required_rag": "NONE",
            "reason": f"라우팅 실패: {type(e).__name__}"
        }
    return state


# ─────────────────────────────────────────────────────────────────────────────
# 5) 단독 실행 테스트
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tests = [
        "저 68세고 의료급여 2종이에요",  # Profile 후보(나이/보험자격)
        "6월에 유방암 C50.9 진단, 항암 치료 중",  # Collection
        "저 68세고 6월에 유방암 C50.9 진단받고 항암 중입니다.",  # BOTH
        "재난적 의료비 신청 가능한가요?",  # NONE 저장, RAG BOTH가 적절(라우터는 NONE/BOTH로 줄 수 있음)
        "안녕하세요"  # NONE/NONE
    ]
    for t in tests:
        s: RouterState = {"user_id": "u1", "input_text": t}
        out = router_node(s)
        print(t, "=>", out["router"])
