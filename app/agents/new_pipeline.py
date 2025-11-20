# app/agents/new_pipeline.py
# -*- coding: utf-8 -*-
"""
service_graph.py

- LangGraph 메인 그래프 정의
- Entry: session_orchestrator
  → query_router
  → info_extractor
  → user_context_node
  → policy_retriever_node
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
from langsmith import traceable

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
        tool_msg = {
            "role": "tool",
            "content": "[session_orchestrator] dummy node executed",
            "created_at": _now_iso(),
                "meta": {
        "no_store": True,  
    },
        }
        return {
            # ★ messages 전체 대신, 이번에 추가할 메시지만 리턴
            "messages": [tool_msg],
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
                    "meta": {
        "no_store": True,  
    },
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
                    "meta": {
        "no_store": True,  
    },
            }],
        }

# 4) user_context_node
try:
    from app.langgraph.nodes.user_context_node import user_context_node as user_context_node
except Exception as e:
    print(f"[service_graph] user_context_node import failed: {e}")
    def user_context_node(state: State) -> Dict[str, Any]:
        # 더미: 프로필/컬렉션 병합 없이 그대로 통과
        ep = state.get("ephemeral_profile") or {}
        ec = state.get("ephemeral_collection") or {}
        return {
            "merged_profile": ep,
            "merged_collection": ec,
            "profile_summary_text": "",
            "history_text": "",
            "rolling_summary": state.get("rolling_summary"),
            "messages": [{
                "role": "tool",
                "content": "[user_context_node] dummy pass-through",
                "created_at": _now_iso(),
                    "meta": {
        "no_store": True,  
    },
            }],
        }

# 5) policy_retriever_node
try:
    from app.langgraph.nodes.policy_retriever import policy_retriever_node as policy_retriever_node
except Exception as e:
    print(f"[service_graph] policy_retriever_node import failed: {e}")
    def policy_retriever_node(state: State) -> Dict[str, Any]:
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
                "content": "[policy_retriever_node] dummy 1 snippet",
                "created_at": _now_iso(),
                    "meta": {
        "no_store": True,  
    },
            }],
        }


# 6) answer_llm
try:
    from app.langgraph.nodes.llm_answer_creator import answer as answer_llm_node
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
        # 기존 messages는 건드리지 않고, 이번 노드 로그만 델타로 리턴
        tool_msg = {
            "role": "tool",
            "content": "[persist_pipeline] dummy; no DB upsert",
            "created_at": _now_iso(),
                "meta": {
        "no_store": True,  
    },
        }
        # counts.messages는 state에 현재까지 쌓인 messages 길이를 쓰는 게 자연스럽기 때문에
        messages_len = len(state.get("messages") or [])

        return {
            "messages": [tool_msg],
            "persist_result": {
                "ok": False,
                "conversation_id": None,
                "counts": {"messages": messages_len, "embeddings": 0},
                "warnings": ["persist_pipeline dummy; DB disabled"],
            },
        }


# ─────────────────────────────────────────────────────────
# 그래프 구성
# ─────────────────────────────────────────────────────────
@traceable
def build_graph():
    graph = StateGraph(State)

    # 노드 등록
    graph.add_node("session_orchestrator", session_orchestrator_node)
    graph.add_node("router", router_node)
    graph.add_node("info_extractor", info_extractor_node)
    graph.add_node("user_context", user_context_node)
    graph.add_node("policy_retriever", policy_retriever_node)
    graph.add_node("answer_llm", answer_llm_node)
    graph.add_node("persist_pipeline", persist_pipeline_node)



    # Entry: 세션 오케스트레이터
    graph.set_entry_point("session_orchestrator")
    graph.add_edge("session_orchestrator", "router")

    # Router → 분기
    def route_edge(state: State):
        nxt = (state or {}).get("next") or "info_extractor"
        action = (state or {}).get("user_action")

        if nxt == "end":
            # 1) 초기화 + 저장 안 함 → 그냥 종료 (persist X)
            if action == "reset_drop":
                return END

            # 2) 초기화 + 저장 OR 기타 end_session=True → persist 후 종료
            if action == "reset_save" or state.get("end_session"):
                return "persist_pipeline"
            # 3) 중간 저장
            if action == "save":
                return "persist_pipeline"
            # 4) 나머지(특수한 end지만 end_session=False) → 그냥 종료
            return END

        if nxt == "retrieval_planner":
            # 라벨은 예전 이름을 유지하고, 실제 노드는 아래 매핑에서 user_context로 연결
            return "retrieval_planner"
        if nxt == "info_extractor":
            return "info_extractor"
        if nxt == "persist_pipeline":
            return "persist_pipeline"
        return "info_extractor"



    graph.add_conditional_edges(
        "router",
        route_edge,
        {
            "info_extractor": "info_extractor",
            "retrieval_planner": "user_context",
            "persist_pipeline": "persist_pipeline",
            END: END,
        },
    )


    # 기본 흐름: IE → user_context → policy_retriever → LLM
    graph.add_edge("info_extractor", "user_context")
    graph.add_edge("user_context", "policy_retriever")
    graph.add_edge("policy_retriever", "answer_llm")


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
        "profile_id": 76,
        "user_input": "췌장암이 있고, 현재 항암치료 중입니다. 제가 받을 수 있는 혜택이 궁금해요",
        "rolling_summary": None,
        "end_session": False,
    }, config=cfg)
    print("answer:", out.get("answer"))
    print("messages_count:", len(out.get("messages", [])))

    print("=== RUN 2 ===")
    out2 = app.invoke({
        "session_id": "sess-001",
        "profile_id": 76,
        "user_input": "저는 B형 간염도 있습니다. 관련된 지원 정책도 알려주세요.",
        "rolling_summary": out.get("rolling_summary"),
        "end_session": False,
    }, config=cfg)
    print("answer2:", out2.get("answer"))
    print("messages_count:", len(out2.get("messages", [])))
    print("=== RUN 3  ===")
    out3 = app.invoke({
        "session_id": "sess-001",
        "profile_id": 76,
        "user_input": "장애인 관련 지원사업 알려주세요.",
        "rolling_summary": out.get("rolling_summary"),
        "end_session": False,
    }, config=cfg)
    print("answer3:", out2.get("answer"))
    print("messages_count:", len(out2.get("messages", [])))
    print("=== RUN 4(save)  ===")
    out4 = app.invoke({
        "session_id": "sess-001",
        "profile_id": 76,
        "user_input": "",
        "rolling_summary": out.get("rolling_summary"),
        "user_action": "save",
    }, config=cfg)
    print("answer4:", out2.get("answer"))
    print("messages_count:", len(out2.get("messages", [])))
    print("persist_result:", out2.get("persist_result"))
    print("done.")
