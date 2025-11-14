# app/langgraph/nodes/query_router.py
# -*- coding: utf-8 -*-
"""
query_router.py

역할:
  - 현재 턴의 user_input을 보고,
    1) 이 발화가 어떤 타입인지 분류
       - POLICY_QA        : 정책/혜택/지원 관련 질문
       - PROFILE_UPDATE   : 프로필(나이·성별·거주지·소득·보험자격·장애등급 등) 정보 제공/갱신
       - MEDICAL_HISTORY  : 병력/진단/수술/입원/투약 등 사례 설명
       - SMALL_TALK       : 인사·잡담
       - OTHER            : 그 외
    2) profile / collection 저장 필요 여부 판단
       - save_profile    : 프로필에 저장할 핵심 조건(나이/소득/보험/장애 등)이 있는지
       - save_collection : 사례/에피소드(언제 어떤 질환/치료 등)가 있는지
    3) RAG 사용 여부 판단
       - use_rag         : 의료·복지 정책/지원/급여/신청 방법 묻는 경우 True

  - 결정 결과는 state["router"]에 dict로 저장:
      {
        "category": "...",
        "save_profile": bool,
        "save_collection": bool,
        "use_rag": bool,
        "reason": "자연어 설명"
      }

  - 그래프 흐름:
      - 현재는 항상 next="info_extractor" 로 넘기되,
        info_extractor / retrieval_planner 가 router 플래그를 참조해
        실제로 어떤 작업을 할지 결정하는 구조로 둔다.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Literal, Optional, TypedDict

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

ROUTER_MODEL = os.getenv("ROUTER_MODEL", "gpt-4o-mini")

_client: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()
    return _client


class RouterDecision(BaseModel):
    """
    LLM이 반환해야 하는 JSON 스키마.
    """
    category: Literal["POLICY_QA", "PROFILE_UPDATE", "MEDICAL_HISTORY", "SMALL_TALK", "OTHER"] = Field(
        description=(
            "발화의 주된 타입.\n"
            "- POLICY_QA: 정책/지원/급여/혜택/신청/자격/조건/서류/절차 등을 묻는 질문\n"
            "- PROFILE_UPDATE: 나이, 생년월일, 성별, 거주지(시/군/구), 건강보험 자격(직장/지역/피부양/의료급여 등), "
            "중위소득 대비 소득수준(예: 120%), 기초생활보장 급여 구분(생계/의료/주거/교육 등), "
            "장애등급/등록장애 여부, 장기요양등급, 임신·출산 여부 등 '상태/속성'을 말하는 경우\n"
            "- MEDICAL_HISTORY: 언제 어떤 진단/수술/입원/투약/검사 등을 받았는지 사례/이력을 설명하는 경우\n"
            "- SMALL_TALK: 인사/감사/잡담/테스트 등\n"
            "- OTHER: 위에 해당하지 않는 기타"
        )
    )
    save_profile: bool = Field(
        description=(
            "이 발화에 프로필에 영구 저장할 가치가 있는 '상태/속성' 정보가 있으면 true.\n"
            "예: 나이, 거주지 구, 소득수준(중위소득 대비), 건강보험 자격, 기초생활급여 구분, 장애등급, 장기요양등급, 임신/출산 여부 등."
        )
    )
    save_collection: bool = Field(
        description=(
            "이 발화에 추후 정책 추천이나 판단에 도움이 될 사례/병력/에피소드가 있으면 true.\n"
            "예: '6월에 유방암 C50.9 진단받고 항암 중', '작년에 뇌졸중으로 입원' 등."
        )
    )
    use_rag: bool = Field(
        description=(
            "정책/지원/급여/사업/신청/절차/서류/문의와 직접 관련된 질문이면 true.\n"
            "예: '재난적 의료비 신청 가능한가요?', '건강보험료 때문에 받을 수 있는 지원이 있나요?' 등."
        )
    )
    reason: str = Field(
        description="왜 이렇게 판단했는지 간단히 한국어로 설명."
    )


class RouterOutput(TypedDict, total=False):
    router: Dict[str, Any]
    next: str
    messages: Any  # StateGraph reducer가 merge


SYSTEM_PROMPT = """
너는 의료·복지 상담 챗봇의 '라우터' 역할을 한다.
사용자의 한 발화를 보고 아래 항목을 판단해 JSON으로만 답하라.

1) category:
   - POLICY_QA        : 의료비 지원, 복지 혜택, 급여, 국가/지자체 사업, 신청 방법/서류/자격/조건을 묻는 질문
   - PROFILE_UPDATE   : 사용자의 '상태/속성' 정보(나이, 생년월일, 성별, 거주지(시/군/구),
                         건강보험 자격, 중위소득 대비 소득(예: 120%), 기초생활보장 급여 구분,
                         장애 등급/등록 여부, 장기요양 등급, 임신/출산 여부 등)를 제공/변경하는 말
   - MEDICAL_HISTORY  : 언제 어떤 질환/진단/수술/입원/검사/치료/투약을 받았는지 사례를 설명하는 말
   - SMALL_TALK       : 인사, 감사, 테스트, 잡담 등
   - OTHER            : 위에 해당하지 않는 경우

2) save_profile (bool):
   - 발화에 프로필에 영구 저장해두면 좋은 '상태/속성' 정보가 있으면 true.
   - 예: '저 68세이고 강북구에 살아요', '의료급여 2종입니다', '중위소득 120% 수준입니다',
         '기초생활보장 생계급여 받고 있어요', '장애 2급입니다', '장기요양등급 3등급이에요', '임신 30주차입니다' 등.

3) save_collection (bool):
   - 발화에 추후 판단에 도움이 될 구체적인 사례/병력이 있으면 true.
   - 예: '6월에 유방암 C50.9 진단받고 항암 치료 중', '작년에 뇌졸중으로 입원했어요',
         '당뇨 합병증으로 투석 중입니다' 등.

4) use_rag (bool):
   - 발화가 '어떤 지원/혜택/급여/정책/사업을 받을 수 있는지', '어디에 어떻게 신청하는지',
     '조건이 되는지'를 묻는 질문이면 true.
   - 단순 정보 제공(프로필/병력만 말하고 질문은 없음)이라면 false.
   - 잡담/테스트도 false.

반드시 아래 형태의 JSON만 출력하라:

{
  "category": "POLICY_QA" | "PROFILE_UPDATE" | "MEDICAL_HISTORY" | "SMALL_TALK" | "OTHER",
  "save_profile": true/false,
  "save_collection": true/false,
  "use_rag": true/false,
  "reason": "..."
}
""".strip()


def _extract_json(text: str) -> str:
    """
    응답 중에서 { ... } JSON 블록만 추출하는 유틸.
    """
    # 가장 처음 나오는 { ... } 블록을 단순 괄호 카운트로 추출
    start = text.find("{")
    if start == -1:
        raise ValueError(f"no '{{' found in: {text}")
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    # 여기까지 오면 비정상
    raise ValueError(f"no matching '}}' found in: {text}")


def _call_router_llm(text: str) -> RouterDecision:
    client = _get_client()
    resp = client.chat.completions.create(
        model=ROUTER_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        temperature=0.0,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content or ""

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # 혹시 모델이 앞뒤에 설명을 덧붙인 경우 대비
        data = json.loads(_extract_json(raw))

    try:
        decision = RouterDecision(**data)
    except ValidationError as e:
        raise ValueError(f"RouterDecision validation failed: {e}\nGot: {data}")
    return decision


@traceable
def route(state: State) -> RouterOutput:
    """
    LangGraph 노드 함수.

    입력:
      - state["user_input"]: 현재 턴 사용자 발화
      - state["messages"]  : 이전까지의 대화 로그

    출력:
      - state["router"]    : RouterDecision dict
      - state["next"]      : "info_extractor" | "retrieval_planner" | "end" (현재는 info_extractor 고정)
      - state["messages"]  : tool 로그 1줄 append
    """
    text = (state.get("user_input") or "").strip()
    msgs: list[Message] = list(state.get("messages") or [])

    # 입력이 비어있으면 그냥 종료 방향
    if not text:
        router_info = {
            "category": "OTHER",
            "save_profile": False,
            "save_collection": False,
            "use_rag": False,
            "reason": "빈 입력이라 아무 작업도 하지 않음",
        }
        msgs.append({
            "role": "tool",
            "content": "[router] empty input → end",
            "created_at": "",
            "meta": {"router": router_info},
        })
        return {
            "router": router_info,
            "next": "end",
            "messages": msgs,
        }

    try:
        decision = _call_router_llm(text)
        router_dict = decision.model_dump()
        log_content = (
            f"[router] category={decision.category}, "
            f"save_profile={decision.save_profile}, "
            f"save_collection={decision.save_collection}, "
            f"use_rag={decision.use_rag}"
        )
        msgs.append({
            "role": "tool",
            "content": log_content,
            "created_at": "",
            "meta": {"router": router_dict},
        })
        # 현재 그래프 구조에서는 기본적으로 info_extractor로 흘려보내고,
        # info_extractor / retrieval_planner 노드가 router 플래그를 참고해서
        # 실제로 프로필/컬렉션 추출과 RAG 여부를 결정하도록 한다.
        next_node = "info_extractor"

    except Exception as e:
        # 실패 시 안전한 폴백
        router_dict = {
            "category": "OTHER",
            "save_profile": False,
            "save_collection": False,
            "use_rag": False,
            "reason": f"router LLM error: {e}",
        }
        msgs.append({
            "role": "tool",
            "content": "[router] error → fallback OTHER",
            "created_at": "",
            "meta": {"error": str(e), "router": router_dict},
        })
        next_node = "info_extractor"

    return {
        "router": router_dict,
        "next": next_node,
        "messages": msgs,
    }


if __name__ == "__main__":
    # 단독 테스트용
    test_inputs = [
        "저 68세고 의료급여 2종이에요",
        "6월에 유방암 C50.9 진단받고 항암 치료 중입니다.",
        "재난적 의료비 신청 가능한가요?",
        "안녕하세요, 테스트입니다.",
    ]
    dummy_state: State = {"messages": []}  # type: ignore
    for t in test_inputs:
        dummy_state["user_input"] = t
        out = route(dummy_state)  # type: ignore
        print("▶", t)
        print("  router:", out["router"])
        print()
