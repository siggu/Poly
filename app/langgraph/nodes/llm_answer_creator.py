# llm_answer_creator.py (Gemini Version)
# 목적: "Answer LLM" 노드
# - RetrievalPlanner의 결과를 받아 최종 답변 생성
# - Google Gemini API를 사용하여 답변 생성

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI
# import google.generativeai as genai

from app.langgraph.state.ephemeral_context import State as GraphState, Message

load_dotenv()

# Gemini API 설정
# genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "gpt-4o-mini")

# ───────────────────────────────────────────────────────────
# 시스템 프롬프트
# ───────────────────────────────────────────────────────────
# SYSTEM_PROMPT = """
# 당신의 임무는 RetrievalPlanner로부터 전달된 문서 목록만을 사용하여 답변하는 것입니다.
# 규칙:
# - 전달된 문서들만 출력합니다.
# - 전달되지 않은 문서는 생성하거나 가정하지 않습니다.
# - 전달된 문서가 6개면 6개 모두 출력하고,
#   전달된 문서가 1개면 1개만 출력합니다.
# - 사용자가 자격이 되는 지원사업만 이미 필터링된 상태로 전달됩니다.
# - 당신은 추가적인 자격 판단을 하지 않습니다.
# - 문서에 있는 요건 및 내용을 기반으로 요약하여 안내합니다.
# - 답변 마지막에 출처 URL을 포함합니다.
# """

# SYSTEM_PROMPT = """
# 당신의 임무는 RetrievalPlanner로부터 전달된 문서 목록만을 사용하여 답변하는 것입니다.

# 규칙(절대 준수):
# - 전달된 문서들만 출력합니다.
# - 전달되지 않은 문서는 생성하거나 추론하지 않습니다.
# - 전달된 document 개수만큼 정확히 같은 개수를 출력합니다.
# - 이미 RetrievalPlanner에서 자격 필터링이 완료된 상태이므로 추가 자격 판단을 하지 않습니다.

# 출력 형식(강제):
# 각 문서는 아래 형식을 그대로 사용하여 출력합니다:

# {문서번호}. {title}
# - 지원 내용: 문서의 "benefits" 또는 snippet 기반으로 요약
# - 지원 자격: 문서의 "requirements" 기반으로 요약
# - 신청 방법: 문서에 존재하면 요약, 없으면 링크 참조
# - 링크: {url}

# 주의:
# - 링크는 각 문서마다 딱 한 번만 출력합니다.
# - 마지막에 전체 URL 목록을 다시 나열하지 않습니다.
# - 지원 내용/자격/신청방법이 문서에 없으면 "제공된 문서에 해당 정보가 없습니다."라고 명시합니다.
# - 문서 순서는 전달받은 순서를 유지합니다.

# 답변 전체 구조:
# 1) 간단한 한 줄 결론
# 2) 위 출력 형식에 따라 문서들을 나열
# 3) 추가 안내(필요한 경우만)
# """
SYSTEM_PROMPT = """
당신은 의료·복지 지원자격 상담사입니다.

## 역할
1. 사용자 프로필(소득, 질환, 장애등급 등)과 제공된 정책 후보를 비교하여 적합한 정책 선별
2. 선별된 정책(상위 3-5개)을 근거로 명확한 답변 생성

## 선별 규칙
- 사용자의 중위소득 비율, 질환, 장애등급, 기초생활보장 수급 여부와 정책 요건을 비교
- 구체적 질환/상태가 정책 요건과 명시적으로 일치하는 경우 우선
- 조건이 명백히 맞지 않는 정책은 제외 (암 환자 정책인데 암 정보 없음 등)
- 적합도 높은 순으로 3-5개 정책만 상세 설명

## 답변 형식
1. **한 줄 결론** (굵게)
2. 각 정책마다:
   - 정책명 + 지역
   - 지원 내용 (benefits 기반)
   - 지원 자격 (requirements 기반)
   - 사용자 정보와의 일치 근거 (소득/질환/등급/지역 비교)
   - 신청 방법 (문서에 있는 경우만)
   - URL (각 정책당 1회만 출력)
3. 다음 단계 안내

## 제약
- 제공된 컨텍스트(Profile/Collection/문서)만 사용, 추측 금지
- 정보 부족 시 "추가 확인 필요" 명시
- 민감 개인정보 요구 금지
- 답변 마지막에 참고 정책 제목과 URL 목록 정리
"""

# ───────────────────────────────────────────────────────────
# 컨텍스트 요약/서식화
# ───────────────────────────────────────────────────────────

def _format_profile_ctx(p: Optional[Dict[str, Any]]) -> str:
    if not p or "error" in p:
        return ""
    lines: List[str] = []

    if p.get("summary"):
        lines.append(f"- 요약: {p['summary']}")

    if p.get("insurance_type"):
        lines.append(f"- 건보 자격: {p['insurance_type']}")

    mir_raw = p.get("median_income_ratio")
    if mir_raw is not None:
        try:
            v = float(mir_raw)
            if v <= 10:
                pct = v * 100.0
            else:
                pct = v
            lines.append(f"- 중위소득 비율: {pct:.1f}%")
        except:
            lines.append(f"- 중위소득 비율: {mir_raw}")

    if (bb := p.get("basic_benefit_type")):
        lines.append(f"- 기초생활보장: {bb}")

    if (dg := p.get("disability_grade")) is not None:
        dg_label = {0: "미등록", 1: "심한", 2: "심하지않음"}.get(dg, str(dg))
        lines.append(f"- 장애 등급: {dg_label}")

    if (lt := p.get("ltci_grade")) and lt != "NONE":
        lines.append(f"- 장기요양 등급: {lt}")

    if p.get("pregnant_or_postpartum12m") is True:
        lines.append("- 임신/출산 12개월 이내")

    return "\n".join(lines)


def _format_collection_ctx(items: Optional[List[Dict[str, Any]]]) -> str:
    if not items:
        return ""
    out = []
    for it in items[:5]:
        if "error" in it:
            continue
        segs = []
        if it.get("predicate"):
            segs.append(f"[{it['predicate']}]")
        if it.get("object"):
            segs.append(it["object"])
        out.append("- " + " ".join(segs))
    return "\n".join(out)


def _format_documents(items: Optional[List[Dict[str, Any]]]) -> str:
    if not items:
        return ""
    out: List[str] = []

    for idx, doc in enumerate(items[:4], start=1):
        if not isinstance(doc, dict):
            continue

        title = doc.get("title") or doc.get("doc_id") or f"문서 {idx}"
        source = doc.get("source")
        score = doc.get("score")
        url = doc.get("url")
        snippet = doc.get("snippet") or ""

        header = f"{idx}. {title}"
        if source:
            header += f" ({source})"
        if score:
            header += f" [score={score:.3f}]"

        out.append(f"- {header}")
        out.append(f"  > {snippet.strip()}")

        if url:
            out.append(f"  출처: {url}")

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

# ───────────────────────────────────────────────────────────
# Gemini LLM 호출
# ───────────────────────────────────────────────────────────

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

    # OpenAI 메시지 구성
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # OpenAI ChatCompletion 호출
        resp = client.chat.completions.create(
            model=ANSWER_MODEL,
            messages=messages,
            temperature=0.3,
            timeout=15.0,  # 타임아웃 설정
        )

        return resp.choices[0].message.content.strip()

    except Exception as e:
        print("🔥🔥 [OpenAI ERROR]", e)
        raise


def run_answer_llm_stream(
    input_text: str,
    used: str,
    profile_ctx: Optional[Dict[str, Any]],
    collection_ctx: Optional[List[Dict[str, Any]]],
    summary: Optional[str] = None,
    documents: Optional[List[Dict[str, Any]]] = None,
):
    """
    스트리밍 방식으로 LLM 응답을 생성합니다.
    제너레이터를 반환하여 청크 단위로 텍스트를 yield합니다.
    """
    user_prompt = _build_user_prompt(
        input_text,
        used,
        profile_ctx,
        collection_ctx,
        summary=summary,
        documents=documents,
    )

    # OpenAI 메시지 구성
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # OpenAI ChatCompletion 스트리밍 호출
        stream = client.chat.completions.create(
            model=ANSWER_MODEL,
            messages=messages,
            temperature=0.3,
            timeout=15.0,
            stream=True,  # 스트리밍 활성화
        )

        # 청크 단위로 yield
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                yield chunk.choices[0].delta.content

    except Exception as e:
        print("🔥🔥 [OpenAI STREAM ERROR]", e)
        yield f"\n\n[오류 발생: {str(e)}]"


# ───────────────────────────────────────────────────────────
# 메시지 컨텍스트 추출
# ───────────────────────────────────────────────────────────

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


# ───────────────────────────────────────────────────────────
# Fallback 메시지
# ───────────────────────────────────────────────────────────

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


# ───────────────────────────────────────────────────────────
# 메인 answer 노드
# ───────────────────────────────────────────────────────────

def answer(state: GraphState) -> Dict[str, Any]:
    messages: List[Message] = list(state.get("messages") or [])
    retrieval = state.get("retrieval") or {}
    ctx = _extract_context_from_messages(messages)

    profile_ctx = ctx.get("profile") or retrieval.get("profile_ctx")
    collection_ctx = ctx.get("collection") or retrieval.get("collection_ctx")

    if isinstance(collection_ctx, dict) and "triples" in collection_ctx:
        collection_ctx_list = collection_ctx["triples"]
    elif isinstance(collection_ctx, list):
        collection_ctx_list = collection_ctx
    else:
        collection_ctx_list = None

    documents = ctx.get("documents") or retrieval.get("rag_snippets")
    summary = ctx.get("summary") or state.get("rolling_summary")

    input_text = (
        (state.get("user_input") or state.get("input_text") or "").strip()
        or _last_user_content(messages).strip()
    )

    used = (retrieval.get("used") or "").strip().upper()
    if not used:
        used = _infer_used_flag(profile_ctx, collection_ctx_list, documents)

    # 스트리밍 모드인 경우 LLM 호출 스킵 (API 레벨에서 처리)
    streaming_mode = state.get("streaming_mode", False)

    if streaming_mode:
        # 스트리밍 모드: 컨텍스트만 저장하고 답변 생성은 스킵
        text = ""  # 빈 텍스트
    else:
        # 일반 모드: LLM 호출하여 답변 생성
        try:
            text = run_answer_llm(
                input_text,
                used,
                profile_ctx,
                collection_ctx_list,
                summary=summary,
                documents=documents,
            )
        except Exception:
            text = _build_fallback_text(
                used,
                profile_ctx,
                collection_ctx_list,
                documents,
                summary,
            )

    citations = {
        "profile": profile_ctx,
        "collection": collection_ctx_list,
        "documents": documents,
    }

    assistant_message: Message = {
        "role": "assistant",
        "content": text,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "model": ANSWER_MODEL,
            "used": used,
            "citations": {
                "profile": bool(profile_ctx),
                "collection_count": len(collection_ctx_list or []),
                "document_count": len(documents or []),
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
        # 스트리밍용 컨텍스트 저장
        "streaming_context": {
            "input_text": input_text,
            "used": used,
            "profile_ctx": profile_ctx,
            "collection_ctx": collection_ctx_list,
            "summary": summary,
            "documents": documents,
        },
    }


def answer_llm_node(state: GraphState) -> Dict[str, Any]:
    return answer(state)
