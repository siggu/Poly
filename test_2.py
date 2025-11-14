# app/langgraph/nodes/retrieval_planner.py
# -*- coding: utf-8 -*-
"""
Retrieval Planner (Full Version)

역할:
  1) Router(required_rag)에 따라 PROFILE / COLLECTION / BOTH / NONE 결정
  2) DB profile + ephemeral_profile  → merge_profile
  3) DB collections + ephemeral_collection → merge_collection
  4) 위 정보 + user_input 기반으로
       - 문서 임베딩(pgvector) similarity 검색
       - 키워드 기반 ILIKE 검색
       - 지역/사이트 가중치 적용
     → 하이브리드 RAG 결과(rag_snippets) 생성
  5) 결과를 state["retrieval"]에 저장

전제:
  - DB: PostgreSQL + pgvector
  - 테이블:
      profiles(id, ...)
      collections(id, profile_id, subject, predicate, object, code_system, code, ...)
      documents(id, title, content, region, source_url, policy_id, ...)
      embeddings(id, document_id, embedding VECTOR, ...)
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from dotenv import load_dotenv

# pgvector 어댑터 (없으면 임베딩 검색은 graceful fallback)
try:
    from pgvector.psycopg import register_vector  # type: ignore
except ImportError:  # pragma: no cover
    register_vector = None  # type: ignore

# 임베딩 모델 (sentence-transformers)
try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except ImportError:  # pragma: no cover
    SentenceTransformer = None  # type: ignore

from app.langgraph.state.ephemeral_context import State
from app.dao.db_user_utils import get_profile_by_id, get_collection_by_profile
from app.langgraph.utils.merge_utils import merge_profile, merge_collection
from app.dao.utils_db import extract_sitename_from_url, get_weight

load_dotenv()

# ─────────────────────────────────────────────────────────
# 환경 설정
# ─────────────────────────────────────────────────────────
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (e.g. postgresql://user:pass@localhost:5432/db)")

if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "dragonkue/bge-m3-ko")

# 전역 임베딩 모델 캐시
_embedding_model: Optional[Any] = None


def _get_embedding_model():
    """
    sentence-transformers 모델 lazy 로딩.
    """
    global _embedding_model
    if _embedding_model is None:
        if SentenceTransformer is None:
            raise RuntimeError(
                "sentence-transformers가 설치되어 있지 않습니다. "
                "pip install sentence-transformers 를 먼저 실행하세요."
            )
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


def _embed_text(text: str) -> List[float]:
    model = _get_embedding_model()
    vec = model.encode([text])[0]
    return vec.tolist()


def _get_conn() -> psycopg.Connection:
    """
    pgvector register까지 포함한 커넥션 생성 유틸.
    """
    conn = psycopg.connect(DB_URL)
    if register_vector is not None:
        register_vector(conn)  # type: ignore
    return conn


# ─────────────────────────────────────────────────────────
# 간단 키워드 추출(LLM 비사용)
# ─────────────────────────────────────────────────────────
def extract_keywords(text: str, max_k: int = 8) -> List[str]:
    if not text:
        return []
    toks = re.findall(r"[가-힣A-Za-z0-9]+", text)
    stop = {
        "그리고", "하지만", "그리고요", "근데",
        "가능한가요", "신청", "문의", "가능", "여부",
        "해당", "있는", "없는", "있나요", "인가요", "혹시",
    }
    norm: List[str] = []
    for t in toks:
        tt = t.lower()
        if (len(tt) >= 2) and (tt not in stop):
            norm.append(tt)

    seen = set()
    out: List[str] = []
    for w in norm:
        if w not in seen:
            seen.add(w)
            out.append(w)
        if len(out) >= max_k:
            break
    return out


# ─────────────────────────────────────────────────────────
# Router + 휴리스틱: RAG 사용 정도 결정
# ─────────────────────────────────────────────────────────
def _decide_required_rag(router: Optional[Dict[str, Any]], input_text: str) -> str:
    """
    1순위: router["required_rag"]가 명시되어 있으면 그대로 사용
    2순위: 간단한 한국어 키워드 휴리스틱
    """
    if router and isinstance(router, dict):
        val = router.get("required_rag")
        if val in {"PROFILE", "COLLECTION", "BOTH", "NONE"}:
            return val

    text = (input_text or "").lower()

    # 자격/지원/혜택/대상 등 정책 질의 느낌 → BOTH
    if any(k in text for k in ["자격", "지원", "혜택", "대상", "되나요", "가능", "요건", "조건"]):
        return "BOTH"

    # 의료/치료/진단 언급 → COLLECTION 위주
    if any(k in text for k in ["진단", "치료", "항암", "투석", "암", "산정특례", "임신", "난임", "문서", "영수증"]):
        return "COLLECTION"

    return "NONE"


# ─────────────────────────────────────────────────────────
# 하이브리드 문서 검색 (임베딩 + 키워드)
# ─────────────────────────────────────────────────────────
def _hybrid_search_documents(
    query_text: str,
    merged_profile: Optional[Dict[str, Any]],
    merged_collection: Optional[Dict[str, Any]],
    top_k: int = 7,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    hybrid search:
    - pgvector similarity 기반
    - region 필터 선택적 적용
    - ordering by embedding distance
    """

    keywords = extract_keywords(query_text, max_k=7)

    # merged_profile.region
    region_filter = None
    if merged_profile:
        region_filter = merged_profile.get("residency_sgg_code")

    # --- 1) 임베딩 생성 ---
    query_vec: Optional[List[float]] = None
    try:
        if query_text.strip():
            query_vec = _embed_text(query_text.strip())
    except Exception:
        query_vec = None

    # 임베딩 검색 실패 → 빈 리스트 반환
    if query_vec is None:
        return [], keywords

    query_embedding_str = str(query_vec)

    # --- 2) SQL query 생성 (너가 준 구조대로) ---
    sql = """
        SELECT
            d.id,
            d.title,
            d.requirements,
            d.benefits,
            d.region,
            d.url,
            1 - (e.embedding <=> CAST(:query_embedding AS vector)) AS similarity
        FROM documents d
        JOIN embeddings e
            ON d.id = e.doc_id
    """

    params = {"query_embedding": query_embedding_str}

    # region filtering
    if region_filter:
        sql += " WHERE d.region = :region"
        params["region"] = region_filter

    sql += f"""
        ORDER BY e.embedding <=> CAST(:query_embedding AS vector)
        LIMIT {top_k}
    """

    # --- 3) DB 실행 ---
    results: List[Dict[str, Any]] = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

            for r in rows:
                results.append(
                    {
                        "doc_id": r[0],
                        "title": r[1],
                        "requirements": r[2],
                        "benefits": r[3],
                        "region": r[4],
                        "url": r[5],
                        "similarity": float(r[6]) if r[6] is not None else 0.0,
                    }
                )

    # similarity 기준 정렬 (높은 순)
    results.sort(key=lambda x: x["similarity"], reverse=True)

    # rag_snippets 형태로 반환
    snippets = []
    for r in results:
        snippets.append(
            {
                "doc_id": r["doc_id"],
                "title": r["title"],
                "requirements": r["requirements"],
                "benefits": r["benefits"],
                "region": r["region"],
                "url": r["url"],
                "score": r["similarity"],
                "score_components": {
                    "similarity": r["similarity"],
                    "region_matched": (region_filter == r["region"]),
                },
            }
        )

    return snippets, keywords



# ─────────────────────────────────────────────────────────
# 메인 노드: retrieval_planner_node
# ─────────────────────────────────────────────────────────
def retrieval_planner_node(state: State) -> State:
    """
    입력:
      - state["user_input"]: 현재 턴 사용자 입력
      - state["router"]: {"required_rag": "..."} (query_router에서 설정)
      - state["profile_id"]: DB profiles.id (세션에 연결된 프로필)
      - state["ephemeral_profile"]: 세션 중 추출된 임시 프로필 정보
      - state["ephemeral_collection"]: 세션 중 추출된 임시 컬렉션(triples)

    동작:
      - required_rag = PROFILE / COLLECTION / BOTH / NONE 결정
      - PROFILE/BOTH:
          DB profile + ephemeral_profile → merge_profile
      - COLLECTION/BOTH:
          DB collections + ephemeral_collection → merge_collection
      - required_rag != NONE:
          하이브리드 문서 검색(임베딩 + 키워드) 수행
      - 결과를 state["retrieval"]에 저장

    출력:
      state["retrieval"] = {
          "used": required_rag,
          "merged_profile": {...} | None,
          "merged_collection": {...} | None,
          "query_text": str,
          "keywords": [ ... ],
          "rag_snippets": [
             {
                "doc_id": ...,
                "title": ...,
                "content": ...,
                "region": ...,
                "source_url": ...,
                "policy_id": ...,
                "score": float,
                "score_components": {...},
             },
             ...
          ],
      }
    """
    input_text = state.get("user_input") or ""
    router_info = state.get("router") or {}
    profile_id = state.get("profile_id")

    ephemeral_profile = state.get("ephemeral_profile") or {}
    ephemeral_collection = state.get("ephemeral_collection") or {}

    required = _decide_required_rag(router_info, input_text)

    merged_profile: Optional[Dict[str, Any]] = None
    merged_collection: Optional[Dict[str, Any]] = None

    # 1) PROFILE 병합
    if required in {"PROFILE", "BOTH"} and profile_id:
        try:
            db_prof = get_profile_by_id(int(profile_id))
            merged_profile = merge_profile(ephemeral_profile, db_prof)
        except Exception as e:
            merged_profile = {
                "error": f"profile_merge_failed: {type(e).__name__}",
                "detail": str(e),
            }

    # 2) COLLECTION 병합
    if required in {"COLLECTION", "BOTH"} and profile_id:
        try:
            db_coll = get_collection_by_profile(int(profile_id))
            merged_collection = merge_collection(ephemeral_collection, db_coll)
        except Exception as e:
            merged_collection = {
                "error": f"collection_merge_failed: {type(e).__name__}",
                "detail": str(e),
            }

    # 3) 문서 RAG 검색
    rag_snippets: List[Dict[str, Any]] = []
    keywords: List[str] = []
    if required != "NONE":
        try:
            rag_snippets, keywords = _hybrid_search_documents(
                query_text=input_text,
                merged_profile=merged_profile,
                merged_collection=merged_collection,
                top_k=8,
            )
        except Exception as e:
            rag_snippets = [
                {
                    "error": f"document_search_failed: {type(e).__name__}",
                    "detail": str(e),
                }
            ]

    # 4) 최종 state에 저장
    state["retrieval"] = {
        "used": required,
        "merged_profile": merged_profile,
        "merged_collection": merged_collection,
        "query_text": input_text,
        "keywords": keywords,
        "rag_snippets": rag_snippets,
    }
    return state


# ─────────────────────────────────────────────────────────
# 단독 실행 테스트 (선택)
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":  # 간단한 수동 테스트용
    from datetime import datetime, timezone
    from pprint import pprint

    def _now():
        return datetime.now(timezone.utc).isoformat()

    dummy_state: State = {
        "session_id": "sess-test-1",
        "user_input": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
        "router": {"required_rag": "BOTH"},
        "profile_id": 1,  # 실제 DB에 존재하는 profiles.id로 바꿔서 테스트
        "ephemeral_profile": {
            "pregnant_or_postpartum12m": {"value": True, "confidence": 0.95},
        },
        "ephemeral_collection": {
            "triples": [
                {
                    "subject": "self",
                    "predicate": "HAS_CONDITION",
                    "object": "유방암",
                    "code_system": "KCD7",
                    "code": "C50.9",
                }
            ]
        },
        "messages": [
            {
                "role": "user",
                "content": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
                "created_at": _now(),
                "meta": {},
            }
        ],
    }

    out = retrieval_planner_node(dummy_state)
    pprint(out.get("retrieval"))
