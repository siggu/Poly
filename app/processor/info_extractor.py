# service_graph.py
from __future__ import annotations
import os
from typing import TypedDict, Any, Dict
from dotenv import load_dotenv
load_dotenv()

# ─────────────────────────────────────────
# LangSmith (traceable + trace context)
# ─────────────────────────────────────────
try:
    from langsmith import traceable
    from langsmith.run_helpers import trace
except Exception:
    # fallback no-op
    def traceable(_func=None, **_kw):
        def deco(f): return f
        return deco
    class _DummyTrace:
        def __call__(self, *a, **kw): return self
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
    trace = _DummyTrace()

# LangGraph
from langgraph.graph import StateGraph, END

# 각 노드 import
from query_router import router_node
from profile_saver import profile_saver_node
from collection_saver import collection_saver_node
from retrieval_planner import retrieval_planner_node
from llm_answer_creator import answer_llm_node


# ─────────────────────────────────────────────────────────────────
# 상태 정의
# ─────────────────────────────────────────────────────────────────
class GraphState(TypedDict, total=False):
    user_id: str
    input_text: str
    router: Dict[str, Any]
    profile_delta: Dict[str, Any]
    triples_delta: Any
    persist_result: Dict[str, Any]
    retrieval: Dict[str, Any]
    answer: Dict[str, Any]


# ─────────────────────────────────────────────────────────────────
# 분기 함수들
# ─────────────────────────────────────────────────────────────────
def _route_after_router(state: GraphState) -> str:
    r = (state.get("router") or {})
    tgt = (r.get("target") or "NONE").upper()
    if tgt == "PROFILE": return "profile_saver"
    if tgt == "COLLECTION": return "collection_saver"
    if tgt == "BOTH": return "profile_saver"
    return "retrieval_planner"

def _route_after_profile(state: GraphState) -> str:
    r = (state.get("router") or {})
    tgt = (r.get("target") or "NONE").upper()
    if tgt == "BOTH": return "collection_saver"
    return "retrieval_planner"


# ─────────────────────────────────────────────────────────────────
# 그래프 빌더 (트레이스)
# ─────────────────────────────────────────────────────────────────
@traceable(name="service_graph.build_graph")
def build_graph():
    g = StateGraph(GraphState)

    # traceable wrapper nodes
    @traceable(name="node.router")
    def router_wrapped(s): return router_node(s)

    @traceable(name="node.profile_saver")
    def profile_wrapped(s): return profile_saver_node(s)

    @traceable(name="node.collection_saver")
    def collection_wrapped(s): return collection_saver_node(s)

    @traceable(name="node.retrieval_planner")
    def retrieve_wrapped(s): return retrieval_planner_node(s)

    @traceable(name="node.answer_llm")
    def answer_wrapped(s): return answer_llm_node(s)

    g.add_node("router", router_wrapped)
    g.add_node("profile_saver", profile_wrapped)
    g.add_node("collection_saver", collection_wrapped)
    g.add_node("retrieval_planner", retrieve_wrapped)
    g.add_node("answer_llm", answer_wrapped)

    g.set_entry_point("router")

    g.add_conditional_edges("router", _route_after_router, {
        "profile_saver": "profile_saver",
        "collection_saver": "collection_saver",
        "retrieval_planner": "retrieval_planner",
    })

    g.add_conditional_edges("profile_saver", _route_after_profile, {
        "collection_saver": "collection_saver",
        "retrieval_planner": "retrieval_planner",
    })

    g.add_edge("collection_saver", "retrieval_planner")
    g.add_edge("retrieval_planner", "answer_llm")
    g.add_edge("answer_llm", END)

    return g.compile()


# ─────────────────────────────────────────────────────────────────
# 실행 함수 (그래프 전체 트레이스)
# ─────────────────────────────────────────────────────────────────
@traceable(name="service_graph.run_once")
def run_once(user_id: str, text: str, *, source_id: str | None = None) -> Dict[str, Any]:
    app = build_graph()
    init_state: GraphState = {
        "user_id": user_id,
        "input_text": text,
    }
    if source_id:
        init_state["source_id"] = source_id

    # 전체 실행 run grouping
    with trace("conversation.run_once"):
        return app.invoke(init_state)


# ─────────────────────────────────────────────────────────────────
# 테스트 실행
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # uid = os.getenv("TEST_USER_ID", "u_demo_1")

    # samples = [
    #     "저 68세고 의료급여 2종입니다. 재난적의료비 대상일까요?",
    #     "6월에 유방암 C50.9 진단받고 항암 치료 중이고, 진단서 있습니다.",
    #     "안녕하세요",
    #     "저 68세고 6월에 유방암 C50.9 진단받고 항암 중입니다.",
    # ]
    uid = os.getenv("TEST_USER_ID", "fbf9f169-7d52-486e-86ca-43310752008d")
    samples = [
        "안녕하세요",
        "저는 25살이고, 여자이고, 성동구 거주중입니다. 현재 임신 3달차이고 생계급여 받고 있습니다.",
        "출산 장려 정책이 궁금합니다."
    ]
    for i, s in enumerate(samples, 1):
        print("\n", "="*80, f"\n[Sample {i}] {s}\n", "="*80)
        out = run_once(uid, s, source_id=f"msg:{i}")
        ans = (out.get("answer") or {}).get("text", "")
        print(ans)
