# answer_llm.py
# 목적: "Answer LLM" 노드
# - Router/Planner 결과(retrieval.used, profile_ctx, collection_ctx)를 받아
#   의료복지 맥락의 응답을 생성한다.
# - 결론(요약) → 근거(프로필/컬렉션 인용) → 다음 단계(증빙/확인/신청 경로) 순서로 답한다.
# - RAG 미사용(NONE)일 때는 일반 규칙 중심의 안전한 가이드만 제공.
#
# 의존:
#   pip install openai python-dotenv
#
# 환경:
#   OPENAI_API_KEY
#   ANSWER_MODEL (기본 gpt-4o-mini)
#
# 입력 state 예:
#   {
#     "user_id": "u1",
#     "input_text": "재난적 의료비 대상인가요? 저는 의료급여2종이에요.",
#     "retrieval": {
#        "used": "BOTH",
#        "profile_ctx": {...},                 # fetch_profile_context 결과
#        "collection_ctx": [ {...}, {...} ]    # fetch_collection_context 결과
#     }
#   }
#
# 출력:
#   state["answer"] = {
#     "text": "<최종 답변 한국어>",
#     "citations": { "profile": {...} | None, "collection": [..] | None },
#     "used": "PROFILE|COLLECTION|BOTH|NONE"
#   }
#
# 메모:
# - 개인 식별정보(정확 주소, 주민번호)는 절대 노출/요청하지 않음.
# - 정책 단정이 어려우면 "가능성/추가 확인 필요"로 안내.
# - SQL 임계치/연도별 기준은 본 모듈에서 하드코딩하지 않음(룰 엔진 외부화 전제).

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI

from app.langgraph.state.ephemeral_context import State as GraphState, Message
from app.langgraph.state.ephemeral_context import State as GraphState, Message

load_dotenv()

ANSWER_MODEL = os.getenv("ANSWER_MODEL", "gpt-4o-mini")
client = OpenAI()

# ─────────────────────────────────────────────────────────────────────────────
# 시스템 프롬프트
# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM_PROMPT = """당신은 의료복지 지원자격 상담사이다.
# 입력(사용자 질문, Profile/Collection 컨텍스트)을 바탕으로 아래 원칙에 맞춘 한국어 답변을 생성한다.

# [스타일]
# - 구조: ①결론(한 줄 요약) → ②근거(인용·수치) → ③다음 단계(증빙/확인/신청 경로)
# - 단정이 어려우면 "가능성 높음/추가 확인 필요"로 표현
# - 근거는 제공된 컨텍스트(Profile/Collection)만 사용. 모르는 사실은 추측 금지
# - 숫자·코드·등급은 원문 그대로 인용(존재할 때만)
# - 지나친 장황함 금지. 단락은 2~5개, 항목은 3~7개

# [보안/프라이버시]
# - 주민번호/정확 주소 등 민감 PII 요구 금지
# - 필요 서류 요청은 유형만 제시(예: 진단서, 산정특례 등록증, 건강보험 자격득실확인서)

# [출력 형식]
# - 마크다운 사용 가능(소제목, 불릿)
# - 대답 내용만 출력(메타 설명 금지)
# """
SYSTEM_PROMPT = """
당신은 의료복지 지원자격 상담사이다.
지침:
- 사용자의 질문에 대해 검색 도구를 사용하여 관련 정보를 찾을 것
- 검색 결과를 바탕으로 명확하고 친절하게 답변할 것
- **사용자 정보(나이, 건강 상태, 소득 수준 등)를 고려하여 해당되는 지원 사업을 우선적으로 추천할 것**
- 지원 대상 요건을 확인하고 사용자가 자격이 되는지 명확히 안내할 것
- 지원 대상, 지원 내용, 신청 방법 등 핵심 정보를 간결하게 요약할 것
- 여러 지역의 정보가 있다면 지역별로 구분하여 안내해야하며 만약 제공된 문서에 세부 지원 내용이 존재한다면 그 내용을 기반으로 답변할 것
- 정보가 부족하면 "해당 정보를 찾을 수 없습니다"라고 솔직히 답변할 것
- 예시 질문 : 암 지원에 대해 알려줘 인 경우 제공 문서에 암 지원이 없으면 참조 하지 않을 것
- 답변 끝에는 출처 URL을 제공하세요.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 컨텍스트 요약/서식화
# ─────────────────────────────────────────────────────────────────────────────
def _format_profile_ctx(p: Optional[Dict[str, Any]]) -> str:
    if not p or "error" in p:
        return ""
    lines = []
    # summary는 retrieval_planner에서 이미 구성됨
    if p.get("summary"):
        lines.append(f"- 요약: {p['summary']}")
    # 핵심 필드 일부를 명시적으로 제공(있을 때만)
    if p.get("insurance_type"):
        lines.append(f"- 건보 자격: {p['insurance_type']}")
    if (mir := p.get("median_income_ratio")) is not None:
        lines.append(f"- 중위소득 비율: {mir:.1f}%")
    if (bb := p.get("basic_benefit_type")):
        lines.append(f"- 기초생활보장: {bb}")
    if (dg := p.get("disability_grade")) is not None:
        dg_label = {0:"미등록",1:"심한",2:"심하지않음"}.get(dg, str(dg))
        lines.append(f"- 장애 등급: {dg_label}")
    if (lt := p.get("ltci_grade")) and lt != "NONE":
        lines.append(f"- 장기요양 등급: {lt}")
    if p.get("pregnant_or_postpartum12m") is True:
        lines.append(f"- 임신/출산 12개월 이내")
    return "\n".join(lines)

def _format_collection_ctx(items: Optional[List[Dict[str, Any]]]) -> str:
    if not items:
        return ""
    out = []
    # 최근 8개까지만 요약
    for it in items[:8]:
        if "error" in it:
            continue
        segs = []
        if it.get("predicate"):
            segs.append(f"[{it['predicate']}]")
        if it.get("object"):
            segs.append(it["object"])
        # 코드/날짜/부정
        code_bits = []
        if it.get("code_system") and it.get("code"):
            code_bits.append(f"{it['code_system']}:{it['code']}")
        if it.get("onset_date"):
            code_bits.append(f"onset={it['onset_date']}")
        if it.get("end_date"):
            code_bits.append(f"end={it['end_date']}")
        if it.get("negation"):
            code_bits.append("negation=true")
        if code_bits:
            segs.append("(" + ", ".join(code_bits) + ")")
        out.append("- " + " ".join(segs))
    return "\n".join(out)

def _format_documents(items: Optional[List[Dict[str, Any]]]) -> str:
    if not items:
        return ""
    out: List[str] = []
    for idx, doc in enumerate(items[:6], start=1):
        if not isinstance(doc, dict):
            continue
        title = (doc.get("title") or doc.get("doc_id") or f"문서 {idx}").strip()
        source = doc.get("source")
        score = doc.get("score")
        header = f"{idx}. {title}"
        if source:
            header += f" ({source})"
        if isinstance(score, (int, float)):
            header += f" [score={score:.3f}]"
        snippet = doc.get("snippet") or doc.get("summary") or ""
        out.append(f"- {header}")
        if snippet:
            out.append(f"  > {str(snippet).strip()[:280]}")
    return "\n".join(out)


def _build_user_prompt(
    input_text: str,
    used: str,
    profile_ctx: Optional[Dict[str, Any]],
    collection_ctx: Optional[List[Dict[str, Any]]],
    summary: Optional[str] = None,
    documents: Optional[List[Dict[str, Any]]] = None,
) -> str:
    prof_block = _format_profile_ctx(profile_ctx)
    coll_block = _format_collection_ctx(collection_ctx)
    doc_block = _format_documents(documents)
    summary_block = (summary or "").strip()

    lines = [f"사용자 질문:\n{input_text.strip()}"]
    lines.append(f"\n[Retrieval 사용: {used}]")

    if prof_block:
        lines.append("\n[Profile 컨텍스트]\n" + prof_block)
    if coll_block:
        lines.append("\n[Collection 컨텍스트]\n" + coll_block)
    if summary_block:
        lines.append("\n[Rolling Summary]\n" + summary_block)
    if doc_block:
        lines.append("\n[RAG 문서 스니펫]\n" + doc_block)

    lines.append("""
요구 출력:
- 맨 앞에 **결론 한 문장**
- 다음에 근거(위 컨텍스트에서만 인용)
- 마지막에 다음 단계(증빙, 추가 확인, 신청 경로)를 간단히
- 추정 금지, 컨텍스트 밖 사실 금지
""")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────────
# LLM 호출
# ─────────────────────────────────────────────────────────────────────────────
def run_answer_llm(
    input_text: str,
    used: str,
    profile_ctx: Optional[Dict[str, Any]],
    collection_ctx: Optional[List[Dict[str, Any]]],
    summary: Optional[str] = None,
    documents: Optional[List[Dict[str, Any]]] = None,
) -> str:
    user_prompt = _build_user_prompt(
        input_text,
        used,
        profile_ctx,
        collection_ctx,
        summary=summary,
        documents=documents,
    )
    resp = client.chat.completions.create(
        model=ANSWER_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
    )
    return (resp.choices[0].message.content or "").strip()

# ─────────────────────────────────────────────────────────────────────────────
# 컨텍스트 보조 함수
# ─────────────────────────────────────────────────────────────────────────────
def _extract_context_from_messages(messages: List[Message]) -> Dict[str, Any]:
    for msg in reversed(messages or []):
        if msg.get("role") != "tool":
            continue
        if msg.get("content") != "[context_assembler] prompt_ready":
            continue
        meta = msg.get("meta") or {}
        ctx = meta.get("context")
        if isinstance(ctx, dict):
            return ctx
    return {}


def _last_user_content(messages: List[Message]) -> str:
    for msg in reversed(messages or []):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def _infer_used_flag(profile_ctx: Any, collection_ctx: Any, documents: Any) -> str:
    has_profile = isinstance(profile_ctx, dict) and bool(profile_ctx)
    has_collection = isinstance(collection_ctx, list) and bool(collection_ctx)
    has_docs = isinstance(documents, list) and bool(documents)
    if has_profile and (has_collection or has_docs):
        return "BOTH"
    if has_profile:
        return "PROFILE"
    if has_collection or has_docs:
        return "COLLECTION"
    return "NONE"


def _safe_json(value: Any, limit: int = 400) -> str:
    if not value:
        return "없음"
    try:
        text = json.dumps(value, ensure_ascii=False)
    except Exception:
        text = str(value)
    return text[:limit] + ("..." if len(text) > limit else "")


def _build_fallback_text(
    used: str,
    profile_ctx: Any,
    collection_ctx: Any,
    documents: Any,
    summary: Optional[str],
) -> str:
    return (
        "죄송해요. 응답 생성 중 문제가 발생했어요.\n\n"
        "## 근거(요약)\n"
        f"- Retrieval 사용: {used}\n"
        f"- Summary: {(summary or '없음')[:400]}\n"
        f"- Profile: {_safe_json(profile_ctx)}\n"
        f"- Collection: {_safe_json(collection_ctx)}\n"
        f"- Documents: {_safe_json(documents)}\n"
        "필요 시 다시 시도해 주세요."
    )


def answer(state: GraphState) -> Dict[str, Any]:
    messages: List[Message] = list(state.get("messages") or [])
    retrieval = state.get("retrieval") or {}
    ctx = _extract_context_from_messages(messages)

    profile_ctx = ctx.get("profile") or retrieval.get("profile_ctx")
    collection_ctx = ctx.get("collection") or retrieval.get("collection_ctx")
    documents = ctx.get("documents") or retrieval.get("rag_snippets")
    summary = ctx.get("summary") or state.get("rolling_summary")

    profile_ctx_dict = profile_ctx if isinstance(profile_ctx, dict) else None
    collection_ctx_list = collection_ctx if isinstance(collection_ctx, list) else None
    documents_list = documents if isinstance(documents, list) else None

    input_text = (
        (state.get("user_input") or state.get("input_text") or "").strip()
        or _last_user_content(messages).strip()
    )
    used = (retrieval.get("used") or "").strip().upper()
    if not used:
        used = _infer_used_flag(profile_ctx_dict, collection_ctx_list, documents_list)

    try:
        text = run_answer_llm(
            input_text,
            used,
            profile_ctx_dict,
            collection_ctx_list,
            summary=summary,
            documents=documents_list,
        )
    except Exception:
        text = _build_fallback_text(
            used,
            profile_ctx_dict,
            collection_ctx_list,
            documents_list,
            summary,
        )

    citations = {
        "profile": profile_ctx_dict,
        "collection": collection_ctx_list,
        "documents": documents_list,
    }

    assistant_message: Message = {
        "role": "assistant",
        "content": text,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "model": ANSWER_MODEL,
            "used": used,
            "citations": {
                "profile": bool(profile_ctx_dict),
                "collection_count": len(collection_ctx_list or []),
                "document_count": len(documents_list or []),
            },
        },
    }

    return {
        "answer": {
            "text": text,
            "citations": citations,
            "used": used,
        },
        "messages": [assistant_message],
    }

# ─────────────────────────────────────────────────────────────────────────────
# LangGraph 상태 & 노드
# ─────────────────────────────────────────────────────────────────────────────
def answer_llm_node(state: GraphState) -> Dict[str, Any]:
    """
    사전 조건:
      - retrieval_planner_node가 state["retrieval"]을 채운 상태
    동작:
      - 컨텍스트를 포맷하여 LLM에 전달 → 응답 생성 → state["answer"] 기록
    """
    return answer(state)

# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 테스트
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 가벼운 데모: Retrieval이 이미 채워졌다고 가정
    demo_state: GraphState = {
        "user_id": "u_demo_1",
        "input_text": "재난적 의료비 대상인지 알고 싶어요. 저는 의료급여2종이고, 최근 유방암 치료 중입니다.",
        "retrieval": {
            "used": "BOTH",
            "profile_ctx": {
                "summary": "건보자격 MEDICAL_AID_2 / 중위소득 45.0% / 장애등급 미등록",
                "insurance_type": "MEDICAL_AID_2",
                "median_income_ratio": 45.0,
                "basic_benefit_type": "MEDICAL",
                "disability_grade": 0,
                "ltci_grade": "NONE",
                "pregnant_or_postpartum12m": False,
            },
            "collection_ctx": [
                {"predicate":"HAS_CONDITION","object":"유방암","code_system":"KCD10","code":"C50.9","onset_date":"2025-06","negation":False,"confidence":0.9,"created_at":"2025-10-01T12:00:00"},
                {"predicate":"UNDER_TREATMENT","object":"항암요법","onset_date":"2025-06","negation":False,"confidence":0.9,"created_at":"2025-10-05T12:00:00"},
                {"predicate":"HAS_DOCUMENT","object":"진단서","confidence":0.8,"created_at":"2025-10-10T12:00:00"}
            ]
        }
    }
    out_state = answer_llm_node(demo_state)
    print(out_state["answer"]["text"])
