#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
app/run_cli.py

간단한 CLI 드라이버:
- LangGraph 서비스 그래프를 불러와 터미널에서 직접 대화 테스트를 수행한다.
- `exit` 또는 `quit` 입력 시 세션을 종료하고 persist_pipeline까지 수행한다.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

from dotenv import load_dotenv

load_dotenv()

os.environ.setdefault("LANGSMITH_TRACING", "true")
os.environ.setdefault("LANGSMITH_PROJECT", "HealthInformer")

from app.agents.service_graph import build_graph  # noqa: E402
from app.langgraph.state.ephemeral_context import State  # noqa: E402
from app.dao.db_user_utils import get_profile_by_id_con


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collect_initial_context() -> Dict[str, Any]:
    """
    CLI 실행 전에 사용자/프로필 정보를 선택적으로 수집한다.
    """
    context: Dict[str, Any] = {}

    print("초기 사용자 정보를 입력할 수 있습니다. 생략하려면 엔터만 누르세요.")
    user_id = input(" - user_id: ").strip()
    if user_id:
        context["user_id"] = user_id

    profile_id_raw = input(" - profile_id (정수): ").strip()
    if profile_id_raw:
        try:
            context["profile_id"] = int(profile_id_raw)
        except ValueError:
            print("   ⚠️  profile_id는 정수가 아니므로 무시합니다.")

    rolling_summary = input(" - 기존 rolling_summary (엔터로 생략): ").strip()
    if rolling_summary:
        context["rolling_summary"] = rolling_summary

    print(" - ephemeral_profile key=value 형태로 입력 (없으면 엔터). 여러 개 입력 가능, 종료는 빈 줄.")
    eph_profile: Dict[str, Any] = {}
    while True:
        line = input("   ephemeral_profile> ").strip()
        if not line:
            break
        if "=" not in line:
            print("     형식은 key=value 입니다.")
            continue
        key, value = (part.strip() for part in line.split("=", 1))
        if not key:
            print("     key는 비워둘 수 없습니다.")
            continue
        eph_profile[key] = value
    if eph_profile:
        context["ephemeral_profile"] = eph_profile

    return context


def _extract_snippet_summary(snippet: Dict[str, Any]) -> str:
    for key in ("snippet", "summary", "description"):
        value = snippet.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    pieces = []
    for key in ("requirements", "benefits", "content"):
        value = snippet.get(key)
        if isinstance(value, str) and value.strip():
            pieces.append(value.strip())

    return " ".join(pieces).strip()


def _collect_rag_snippets(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    snippets: List[Dict[str, Any]] = []

    def _extend(candidates: Any) -> None:
        if isinstance(candidates, list):
            for item in candidates:
                if isinstance(item, dict):
                    snippets.append(item)

    retrieval = result.get("retrieval")
    if isinstance(retrieval, dict):
        _extend(retrieval.get("rag_snippets"))

    _extend(result.get("rag_snippets"))

    answer = result.get("answer")
    if isinstance(answer, dict):
        citations = answer.get("citations")
        if isinstance(citations, dict):
            _extend(citations.get("documents"))

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for snippet in snippets:
        key = (
            str(snippet.get("doc_id") or "").strip(),
            str(snippet.get("title") or "").strip(),
            str(snippet.get("snippet") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(snippet)

    return deduped


def _print_rag_snippets(result: Dict[str, Any]) -> None:
    if not isinstance(result, dict):
        return

    snippets = _collect_rag_snippets(result)
    router = result.get("router") if isinstance(result.get("router"), dict) else None

    raw_retrieval = result.get("retrieval")
    retrieval = raw_retrieval if isinstance(raw_retrieval, dict) else None
    used_flag = None
    if isinstance(retrieval, dict):
        used_flag = retrieval.get("used") or retrieval.get("used_flag") or retrieval.get("used_rag")
    if not used_flag:
        answer = result.get("answer")
        if isinstance(answer, dict):
            used_flag = answer.get("used")

    header = "\n[참조 RAG 스니펫]"
    if used_flag is not None:
        header += f" (used={used_flag})"
    print(header)

    if not snippets:
        use_rag = None
        router_reason = None
        if router:
            use_rag = router.get("use_rag")
            router_reason = router.get("reason")

        keywords = None
        if retrieval:
            keywords = retrieval.get("keywords")

        debug_parts = []
        if use_rag is not None:
            debug_parts.append(f"use_rag={use_rag}")
        if retrieval and "used_rag" in retrieval:
            debug_parts.append(f"used_rag={retrieval.get('used_rag')}")
        if keywords:
            debug_parts.append(f"keywords={keywords}")
        if router_reason:
            debug_parts.append(f"router_reason={router_reason}")
        if retrieval is None:
            debug_parts.append(f"retrieval_key={type(raw_retrieval).__name__}")
            debug_parts.append(f"result_keys={sorted(result.keys())}")
        else:
            debug_parts.append(f"retrieval_keys={sorted(retrieval.keys())}")
            debug_parts.append(f"retrieval_rag_snippets_len={len(retrieval.get('rag_snippets') or [])}")

        if debug_parts:
            print(f" (현재 참조 중인 스니펫이 없습니다. {'; '.join(debug_parts)})\n")
        else:
            print(" (현재 참조 중인 스니펫이 없습니다.)\n")

        messages = result.get("messages")
        tool_logs: List[str] = []
        if isinstance(messages, list):
            for msg in messages[-6:]:
                if not isinstance(msg, dict):
                    continue
                if msg.get("role") != "tool":
                    continue
                content = (msg.get("content") or "").strip()
                if content:
                    tool_logs.append(content)
        if tool_logs:
            print("[디버그] 최근 tool 메시지:")
            for log in tool_logs:
                print(f"  - {log}")
            print()
        return

    for idx, snippet in enumerate(snippets, start=1):
        title = snippet.get("title") or snippet.get("doc_id") or f"문서 {idx}"
        line = f" {idx}. {title}"

        score = snippet.get("score")
        if isinstance(score, (int, float)):
            line += f" (score: {score:.3f})"

        print(line)

        summary = _extract_snippet_summary(snippet)
        if summary:
            if len(summary) > 240:
                summary = summary[:240].rstrip() + "…"
            print(f"    - 요약: {summary}")

        url = snippet.get("url")
        if isinstance(url, str) and url.strip():
            print(f"    - URL: {url.strip()}")

    print()


def _print_answer(answer: Any) -> None:
    if not answer:
        return
    if isinstance(answer, dict):
        text = answer.get("text") or ""
    else:
        text = str(answer)
    if text:
        print(f"봇> {text}\n")


def run_cli() -> None:
    graph = build_graph()

    session_id = f"cli-{uuid4().hex[:12]}"
    cfg = {"configurable": {"thread_id": session_id}}

    initial_ctx = _collect_initial_context()

    profile_snapshot = None
    profile_id = initial_ctx.get("profile_id")
    if profile_id is not None:
        profile_snapshot = get_profile_by_id_con(int(profile_id))
        if profile_snapshot:
            print("\n[프로필 조회 결과]")
            for key, value in profile_snapshot.items():
                print(f"  {key}: {value}")
            print()
        else:
            print("\n⚠️  지정한 profile_id에 해당하는 프로필을 찾을 수 없습니다.\n")

    state: State = {  # type: ignore
        "session_id": session_id,
        "messages": initial_ctx.get("messages") or [],
        "rolling_summary": initial_ctx.get("rolling_summary"),
        "turn_count": 0,
        "end_session": False,
    }
    for key in ("user_id", "profile_id", "ephemeral_profile", "ephemeral_collection"):
        if key in initial_ctx:
            state[key] = initial_ctx[key]  # type: ignore[index]

    if profile_snapshot and "ephemeral_profile" not in state:
        state["ephemeral_profile"] = profile_snapshot

    if state.get("rolling_summary") is None:
        state["rolling_summary"] = None

    print("=== HealthInformer CLI ===")
    print("메시지를 입력하면 LangGraph가 응답합니다. 종료하려면 'exit' 또는 'quit'을 입력하세요.\n")

    try:
        while True:
            try:
                user_text = input("사용자> ").strip()
            except EOFError:
                user_text = "exit"

            if not user_text:
                continue

            should_end = user_text.lower() in {"exit", "quit"}
            state["end_session"] = should_end
            state["user_input"] = user_text

            messages = list(state.get("messages") or [])
            messages.append({
                "role": "user",
                "content": user_text,
                "created_at": _now_iso(),
                "meta": {},
            })
            state["messages"] = messages

            result: Dict[str, Any] = graph.invoke(state, config=cfg)
            _print_rag_snippets(result)
            _print_answer(result.get("answer"))

            state = result  # type: ignore[assignment]
            state["messages"] = list(state.get("messages") or [])
            state["user_input"] = None

            if should_end:
                persist = state.get("persist_result")
                if persist:
                    print(f"[persist] {persist}")
                break

    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
    finally:
        print("세션을 종료합니다.")


if __name__ == "__main__":
    run_cli()

