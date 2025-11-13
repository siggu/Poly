# app/agents/service_graph.py
# -*- coding: utf-8 -*-
"""
service_graph.py

- LangGraph 메인 그래프 정의
- Entry: session_orchestrator
  → query_router
  → info_extractor
  → retrieval_planner
  → context_assembler
  → answer_llm
  → (end_session=True이면) persist_pipeline

주의:
  - State 스키마는 app.langgraph.state.ephemeral_context.State 를 단일 소스로 사용
  - 일부 노드는 아직 미구현일 수 있어 try/except 로 더미 구현을 제공
"""

from __future__ import annotations

import os, sys
from datetime import datetime, timezone
from typing import Any, Dict

# 프로젝트 루트 경로를 sys.path에 추가
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    
from dotenv import load_dotenv
load_dotenv()

# LangGraph
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# 공통 State / 타입
from app.langgraph.state.ephemeral_context import State, Message, RagSnippet

# ─────────────────────────────────────────────────────────
# 환경 변수
# ─────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "dragonkue/bge-m3-ko")

if os.getenv("LANGSMITH_TRACING", "false").lower() == "true":
    os.environ["LANGSMITH_API_KEY"] = os.getenv("LANGSMITH_API_KEY", "")
    os.environ["LANGSMITH_ENDPOINT"] = os.getenv("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com")
    os.environ["LANGSMITH_PROJECT"] = os.getenv("LANGSMITH_PROJECT", "pr-medical-chatbot")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────
# 노드 import (없으면 더미로 대체)
# ─────────────────────────────────────────────────────────

# 1) session_orchestrator
try:
    from app.langgraph.nodes.session_orchestrator import orchestrate as session_orchestrator_node
except Exception:
    def session_orchestrator_node(state: State) -> Dict[str, Any]:
        msgs = list(state.get("messages") or [])
        msgs.append({
            "role": "tool",
            "content": "[session_orchestrator] dummy node executed",
            "created_at": _now_iso(),
            "meta": {},
        })
        return {
            "messages": msgs,
            "session_id": state.get("session_id") or "sess-dummy",
            "end_session": False,
            "started_at": state.get("started_at") or _now_iso(),
            "last_activity_at": _now_iso(),
            "turn_count": int(state.get("turn_count") or 0) + 1,
        }

# 2) query_router
try:
    from app.langgraph.nodes.query_router import route as router_node
except Exception:
    def router_node(state: State) -> Dict[str, Any]:
        """
        더미 Router:
        - user_input이 비어있으면 next="end"
        - 아니면 info_extractor로 보냄
        """
        ui = (state.get("user_input") or "").strip()
        if not ui:
            return {"next": "end"}
        return {
            "next": "info_extractor",
            "messages": [{
                "role": "tool",
                "content": f"[router] dummy route → info_extractor (user_input='{ui[:30]}')",
                "created_at": _now_iso(),
                "meta": {},
            }],
        }

# 3) info_extractor
try:
    from app.langgraph.nodes.info_extractor import extract as info_extractor_node
except Exception:
    def info_extractor_node(state: State) -> Dict[str, Any]:
        ep = dict(state.get("ephemeral_profile") or {})
        ec = dict(state.get("ephemeral_collection") or {})
        text = (state.get("user_input") or "").lower()

        # 아주 간단한 더미 추출 예시
        if "소득" in text:
            ep["income_hint"] = {"value": "MENTIONED", "confidence": 0.9}
        if "당뇨" in text:
            ep["disease_codes"] = [{"value": "E11", "confidence": 0.95}]

        ec.setdefault("interests", [])
        if "지원" in text or "혜택" in text:
            ec["interests"] = list(set(ec["interests"] + ["복지지원"]))

        return {
            "ephemeral_profile": ep,
            "ephemeral_collection": ec,
            "messages": [{
                "role": "tool",
                "content": "[info_extractor] dummy updated profile/collection",
                "created_at": _now_iso(),
                "meta": {},
            }],
        }

# 4) retrieval_planner
try:
    from app.langgraph.nodes.retrieval_planner import plan as retrieval_planner_node
except Exception:
    def retrieval_planner_node(state: State) -> Dict[str, Any]:
        # 더미 RAG 스니펫 1개
        snippets: list[RagSnippet] = [{
            "doc_id": "doc-1",
            "source": "policy_db",
            "title": "고혈압·당뇨 합병증 관리사업",
            "snippet": "서울시 거주 65세 이상, 중위소득 120% 이하 고혈압·당뇨 환자 대상...",
            "score": 0.83,
        }]
        return {
            "rag_snippets": snippets,
            "retrieval_meta": {
                "filters": {},
                "k": 1,
                "elapsed_ms": 0,
            },
            "messages": [{
                "role": "tool",
                "content": "[retrieval_planner] dummy 1 snippet",
                "created_at": _now_iso(),
                "meta": {},
            }],
        }

# 5) context_assembler
try:
    from app.langgraph.nodes.context_assembler import assemble as context_assembler_node
except Exception:
    def context_assembler_node(state: State) -> Dict[str, Any]:
        msgs = state.get("messages") or []
        last_user = next((m["content"] for m in reversed(msgs) if m.get("role") == "user"), "")
        summary = state.get("rolling_summary") or ""
        rag = state.get("rag_snippets") or []
        preview = f"[SUMMARY]{summary[:60]}\n[USER]{last_user[:60]}\n[RAG]{len(rag)} snippets"
        return {
            "messages": [{
                "role": "tool",
                "content": "[context_assembler] prompt_ready",
                "created_at": _now_iso(),
                "meta": {"preview": preview},
            }],
        }

# 6) answer_llm
try:
    from app.langgraph.nodes.answer_llm import answer as answer_llm_node
except Exception:
    def answer_llm_node(state: State) -> Dict[str, Any]:
        ui = state.get("user_input") or ""
        ans = f"(더미 응답) 질문을 받았어요: {ui[:60]}"
        return {
            "answer": ans,
            "messages": [{
                "role": "assistant",
                "content": ans,
                "created_at": _now_iso(),
                "meta": {},
            }],
        }

# 7) persist_pipeline
try:
    from app.langgraph.nodes.persist_pipeline import persist as persist_pipeline_node
except Exception:
    def persist_pipeline_node(state: State) -> Dict[str, Any]:
        msgs = list(state.get("messages") or [])
        msgs.append({
            "role": "tool",
            "content": "[persist_pipeline] dummy; no DB upsert",
            "created_at": _now_iso(),
            "meta": {},
        })
        return {
            "messages": msgs,
            "persist_result": {
                "ok": False,
                "conversation_id": None,
                "counts": {"messages": len(msgs), "embeddings": 0},
                "warnings": ["persist_pipeline dummy; DB disabled"],
            },
        }


# ─────────────────────────────────────────────────────────
# 그래프 구성
# ─────────────────────────────────────────────────────────

def build_graph():
    graph = StateGraph(State)

    # 노드 등록
    graph.add_node("session_orchestrator", session_orchestrator_node)
    graph.add_node("router", router_node)
    graph.add_node("info_extractor", info_extractor_node)
    graph.add_node("retrieval_planner", retrieval_planner_node)
    graph.add_node("context_assembler", context_assembler_node)
    graph.add_node("answer_llm", answer_llm_node)
    graph.add_node("persist_pipeline", persist_pipeline_node)

    # Entry: 세션 오케스트레이터
    graph.set_entry_point("session_orchestrator")
    graph.add_edge("session_orchestrator", "router")

    # Router → 분기
    def route_edge(state: State):
        nxt = (state or {}).get("next") or "info_extractor"
        if nxt == "end":
            # end_session 여부에 따라 persist 또는 END
            return "persist_pipeline" if state.get("end_session") else END
        if nxt == "retrieval_planner":
            return "retrieval_planner"
        if nxt == "info_extractor":
            return "info_extractor"
        return "info_extractor"

    graph.add_conditional_edges(
        "router",
        route_edge,
        {
            "info_extractor": "info_extractor",
            "retrieval_planner": "retrieval_planner",
            "persist_pipeline": "persist_pipeline",
            END: END,
        },
    )

    # 기본 흐름: IE → RP → CA → LLM
    graph.add_edge("info_extractor", "retrieval_planner")
    graph.add_edge("retrieval_planner", "context_assembler")
    graph.add_edge("context_assembler", "answer_llm")

    # answer_llm → 세션 종료 여부에 따라 persist 혹은 END
    def maybe_persist(state: State):
        return "persist_pipeline" if state.get("end_session") else END

    graph.add_conditional_edges(
        "answer_llm",
        maybe_persist,
        {
            "persist_pipeline": "persist_pipeline",
            END: END,
        },
    )

    # 인메모리 체크포인터 (thread_id 필요)
    checkpointer = MemorySaver()
    return graph.compile(checkpointer=checkpointer)


# ─────────────────────────────────────────────────────────
# 샘플 실행
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = build_graph()

    cfg = {"configurable": {"thread_id": "sess-001"}}

    print("=== RUN 1 ===")
    out = app.invoke({
        "session_id": "sess-001",
        "profile_id": 1,
        "user_input": "저는 중위소득 120%이고 당뇨가 있어요. 받을 수 있는 의료 지원이 궁금해요.",
        "messages": [{
            "role": "user",
            "content": "안녕하세요",
            "created_at": _now_iso(),
            "meta": {},
        }],
        "rolling_summary": None,
        "end_session": False,
    }, config=cfg)
    print("answer:", out.get("answer"))
    print("messages_count:", len(out.get("messages", [])))

    print("=== RUN 2 (END SESSION) ===")
    out2 = app.invoke({
        "session_id": "sess-001",
        "profile_id": 1,
        "user_input": "오늘 대화 저장해줘.",
        "messages": out.get("messages"),
        "rolling_summary": out.get("rolling_summary"),
        "end_session": True,
    }, config=cfg)
    print("answer2:", out2.get("answer"))
    print("messages_count:", len(out2.get("messages", [])))
    print("persist_result:", out2.get("persist_result"))
    print("done.")
