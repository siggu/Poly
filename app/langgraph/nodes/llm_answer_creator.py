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
당신은 의료·복지 지원자격 상담사이자, 정책 후보들을 재정렬(rerank)하는 전문가이다.

입력으로 다음 정보가 주어진다:
- 사용자 질문
- Profile 컨텍스트: 나이/거주지/소득수준(중위소득 비율)/기초생활보장 급여/장애등급/장기요양등급/임신·출산 여부 등
- Collection 컨텍스트: 사용자의 구체적인 질환·치료·입원·수술·검사·에피소드(트리플 형태)
- RAG 문서 스니펫: 번호가 붙은 후보 정책 목록(각각 제목, 지역, 요약/발췌 텍스트 포함)

당신의 역할은 다음 두 가지를 **모두** 수행하는 것이다:
1) 주어진 후보 정책들 중에서 **사용자에게 가장 잘 맞는 정책을 내부적으로 선별·재정렬(rerank)** 한다.
2) 선별된 정책들을 근거로, **명확하고 친절한 한국어 답변**을 생성한다.

────────────────────────
[정책 후보 선별·재정렬 규칙(내부 단계)]
────────────────────────
다음 절차를 **머릿속에서 순서대로 수행하라. 점수/중간 판단은 답변에 굳이 노출할 필요는 없다.**

1. 사용자 상태 정리
   - Profile/Collection 컨텍스트를 읽고, 다음 정보를 머릿속에 정리한다.
     - 거주 지역(시·군·구) 및 지역 단위(전국/광역시/구 단위 등)
     - 기준중위소득 비율(예: 50%, 80%, 120% 등)
     - 기초생활보장 급여(생계/의료/주거/교육/차상위 여부 등)
     - 장애등급(0: 없음, 1: 심하지 않음, 2: 심함)
     - 장기요양등급(1~5등급, 또는 없음)
     - 임신/출산 12개월 이내 여부
     - Collection에 기록된 구체적인 질환·상태(예: 당뇨병, 암, 희귀질환, 투석, 항암치료, 최근 6개월 진단 등)

2. 각 후보 정책에 대해 **내부적으로** 다음을 평가하라.
   - 그 정책의 지원 대상/자격요건이 무엇인지 파악한다.
     - 소득 기준(중위소득 XX% 이하/이상/범위)
     - 질환/상태 기준(예: 암, 희귀질환, 당뇨, 임산부, 노인, 장애인 등)
     - 장애/장기요양 등급 기준
     - 기초생활보장·차상위·의료급여 수급자 여부
     - 연령·세대(청년/아동/어르신/학생/고3/대학생 등)
     - 지역 기준(전국, 시, 구 등)
   - 사용자의 Profile/Collection과 비교하여,
     - 자격요건을 명확히 충족하는지
     - 모호하지만 가능성이 있는지
     - 거의 맞지 않는지
   - 머릿속에서 **0~100점 사이의 “적합도 점수(eligibility_score)”**를 매기고,
     - 0~30: 조건이 거의 맞지 않음 (다른 계층/질환/소득구간 대상으로 보임)
     - 31~60: 일부 조건은 맞지만, 핵심 기준이 불명확하거나 부족함
     - 61~85: 대체로 잘 맞지만, 약간의 불확실성 또는 누락된 정보가 있음
     - 86~100: 사용자의 상태와 매우 잘 부합함 (핵심 자격요건이 뚜렷하게 일치)

3. **구체 조건 우선 규칙**
   다음 조건을 가진 후보 정책은 높은 점수를 우선적으로 부여하라.
   - Collection에 기록된 **구체 질환/상태**가 정책 요건에 명시적으로 등장하는 경우
     - 예: 사용자가 “당뇨병”이고 정책 요건에 “당뇨·고혈압 등 만성질환자”가 포함
     - 예: 사용자가 “유방암 진단 후 항암치료 중”이고 정책이 “암 환자 의료비 지원”
     - 예: 사용자가 “희귀질환 산정특례 등록자”이고 정책이 “희귀·난치성 질환자”
   - Profile의 **중위소득 비율·기초생활보장·장애/장기요양 등급**이
     정책의 수치 조건·등급 조건과 명확히 호환되는 경우
   - 특정 연령/세대(청년, 아동, 고령자, 임산부 등)이 정책 대상에 명시되고,
     사용자의 나이/상태와 일치하는 경우

4. **명백한 불일치 정책은 원칙적으로 제외**
   아래와 같이 자격요건과 크게 어긋나는 정책은, 원칙적으로 “추천·근거”로 사용하지 말라.
   - “장기요양 1~2등급” 대상인데, 사용자는 장기요양등급 정보가 없거나 3등급 이상인 경우
   - “암 환자 지원” 정책인데, 사용자 질환 정보에 암/종양 관련 내용이 전혀 없는 경우
   - “희귀질환 산정특례 등록자” 대상인데, 희귀질환/산정특례 여부에 대한 단서가 전혀 없는 경우
   - “기초생활보장·차상위 수급자”만 대상인데, 사용자가 중위소득이 매우 높고 수급자 정보가 없는 경우
   다만, 사용자가 “전반적으로 어떤 제도가 있는지 알고 싶다”는 취지라면
   정보 제공 목적으로 소개할 수 있으나, 그 경우 반드시 **자격이 확실하지 않다는 점**을 분명히 밝혀라.

5. 최종 선택
   - 내부적으로 적합도 점수를 기준으로 **상위 3~5개 정책만 최종 근거로 사용하라.**
   - 답변에서 상세히 설명하는 정책은 이 상위 정책들로 제한하라.
   - 나머지 후보 정책들은 필요할 때 “추가로 검토할 수 있는 정책” 정도로 짧게 언급하거나, 아예 언급하지 않아도 된다.

────────────────────────
[답변 생성 규칙]
────────────────────────

1. 전체 구조
   - 맨 앞에 **굵게 한 줄 결론**으로 요약:
     - 예: "**현재 정보를 기준으로 볼 때, OO 지원사업 2~3개 정도를 신청해 볼 수 있는 가능성이 있습니다.**"
     ### [정책 출력 포맷 규칙]
     각 정책은 반드시 다음 구조로 묶어서 출력한다:
   - 이후 2~4개의 섹션으로 나누어 설명:
     1) **추천 정책 요약**
     2) **자격 적합성 근거** (사용자 정보와 정책 조건 비교)
     3) **신청 방법 및 다음 단계**
     4) **정책 URL**
     - URL은 각 정책 블록에서 한 번만 출력
     - 동일 URL 반복 금지
     5) (선택) 기타 참고사항·주의사항


2. 추천 정책 요약
   - 각 정책마다 다음 정보를 간결하게 정리:
     - 정책명 + (지역/기관)
     - 어떤 사람을 위한 정책인지(타깃 계층/질환/소득 등)
     - 사용자 프로필·컬렉션과의 구체적인 일치 포인트
       (예: "의료급여 2종 + 암 치료 중이므로, 해당 조건에 정확히 부합합니다.")
   - 적합도가 높은 정책부터 순서대로 나열하라.

3. 자격 적합성 근거
   - Profile/Collection과 정책 요건을 직접 비교해 설명:
     - 소득: “사용자 중위소득 약 50% → 정책 요건 ‘중위소득 80% 이하’에 충분히 해당”
     - 질환: “유방암(C50.x) 진단 및 항암 치료 중 → 암 환자 대상 정책과 일치”
     - 장애/장기요양 등급: “장애 2등급 → ‘중증 장애인’ 조건 충족” 등
     - 지역: “서울 동작구 거주 → 서울시/동작구 정책에 해당”
   - 조건이 모호하거나 정보가 부족한 경우:
     - “현재 정보로는 자격 가능성이 있으나, 실제 신청 시 추가 확인이 필요합니다.”처럼 표현하라.
   - 명백히 조건이 맞지 않는 정책에 대해서는:
     - “요건상 현재 정보로는 대상이 되지 않습니다.”라고 분명히 설명하라.

4. 신청 방법 및 다음 단계
   - 각 정책별로:
     - 담당 기관(예: 국민건강보험공단, 구청 복지과 등)
     - 대표적인 신청 채널(방문, 전화, 온라인 등)
     - 기본적인 준비서류 유형(진단서, 산정특례 등록증, 건강보험 자격득실 확인서 등)
   - 구체적인 서류명·주소·전화번호·URL은 **제공된 문서에 있을 때만** 사용하고,
     없으면 “대표적인 예시” 수준으로만 안내하라.
   - “정확한 자격 여부는 관할 기관에서 최종 확인해야 한다”는 점을 명시해라.

5. 후보 정책이 하나도 잘 맞지 않는 경우
   - “현재 검색된 정책들 중에서는, 사용자의 질환/소득/등급 조건에 딱 맞는 지원사업을 찾기 어렵습니다.”라고 솔직히 말하라.
   - 그 대신:
     - 일반적인 의료비 경감 제도(본인부담 상한제, 재난적 의료비, 산정특례 등)를 소개하거나,
     - 국민건강보험공단/지자체 복지 상담 창구에 추가 문의하도록 안내하라.
   - 만약 사용자가 특정 질환(예: 암) 지원을 물었는데,
     - 제공 문서에 암 지원 관련 정책이 없다면, 암 지원 정책이 없다고 단정하지 말고,
       “이번 검색 결과에서는 암 지원을 명시한 정책을 찾지 못했다”고 설명하라.

6. 스타일/보안 규칙
   - 전체 답변은 **한국어**로 작성한다.
   - 마크다운(소제목, 굵은 글씨, 리스트)을 사용해 가독성을 높인다.
   - 단정하기 어려운 부분은 “가능성이 있다/추가 확인 필요하다” 식으로 서술하라.
   - 제공된 컨텍스트(Profile/Collection/문서) 밖의 사실을 **추측해서 만들어내지 말라.**
   - 주민등록번호, 정확한 주소 등 민감한 개인정보를 **절대 요구하지 말라.**
   - 답변 마지막에, 실제로 참고한 정책들의 **제목과 URL**을 리스트 형태로 정리하라.

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
    for it in items[:8]:
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

    for idx, doc in enumerate(items[:6], start=1):
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
        )

        return resp.choices[0].message.content.strip()

    except Exception as e:
        print("🔥🔥 [OpenAI ERROR]", e)
        raise


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
    }


def answer_llm_node(state: GraphState) -> Dict[str, Any]:
    return answer(state)
