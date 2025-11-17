# app/langgraph/nodes/context_assembler.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List
from datetime import datetime, timezone
from langchain_openai import ChatOpenAI

SUMMARY_PROMPT = """
다음은 지금까지의 대화 요약입니다:
<OLD_SUMMARY>
{old_summary}
</OLD_SUMMARY>

아래는 최근 턴의 메시지들입니다:
<RECENT_MESSAGES>
{recent_messages}
</RECENT_MESSAGES>

사용자가 추후 질문을 해도 컨텍스트를 잃지 않도록,
중요한 정보만 간결하고 명확하게 요약해 주세요.
"""

def _summarize(old_summary: str, messages: List[Dict[str, Any]]) -> str:
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    recent_text = "\n".join(
        f"{m['role']}: {m['content']}" for m in messages[-8:]
    )

    prompt = SUMMARY_PROMPT.format(
        old_summary=old_summary or "",
        recent_messages=recent_text,
    )

    out = llm.invoke(prompt)
    return out.content.strip()


def assemble(state: Dict[str, Any]) -> Dict[str, Any]:
    messages = state.get("messages", [])
    previous_summary = state.get("rolling_summary")
    turn_count = int(state.get("turn_count") or 1)

    # summary 업데이트 여부
    should_update = (turn_count % 15 == 0)

    if should_update:
        new_summary = _summarize(previous_summary, messages)
    else:
        new_summary = previous_summary

    # Retrieval 결과 (profile, collection, 문서)
    retrieval = state.get("retrieval", {})
    profile_ctx = retrieval.get("profile_ctx")
    collection_ctx = retrieval.get("collection_ctx")
    rag_snippets = retrieval.get("rag_snippets") or []

    # answer_llm이 직접 읽을 컨텍스트
    context_block = {
        "profile": profile_ctx,
        "collection": collection_ctx,
        "documents": rag_snippets,
        "summary": new_summary,
    }

    # tool 메시지 추가 (history 유지!)
    assembled_msg = {
        "role": "tool",
        "content": "[context_assembler] prompt_ready",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "summary_updated": should_update,
            "turn_count": turn_count,
        }
    }

    # messages는 누적 유지해야 함!
    new_messages = messages + [assembled_msg]

    return {
        "rolling_summary": new_summary,
        "context": context_block,   # ★ answer_llm이 반드시 읽을 수 있도록 직접 넘김
        "messages": new_messages,   # ★ history 유지 + 새 메시지 추가
    }
