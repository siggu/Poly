# -*- coding: utf-8 -*-
# retrieval_planner.py
# -------------------------------------------------------------------
# 기능:
#   1) ephemeral + DB 프로필/컬렉션 merge
#   2) SentenceTransformer(bge-m3-ko) 임베딩
#   3) pgvector 기반 문서 검색 (use_rag=True일 때만)
#   4) state["retrieval"]에 profile_ctx, collection_ctx, rag_snippets 저장
# -------------------------------------------------------------------

from __future__ import annotations

import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# LangSmith trace 데코레이터 (없으면 no-op)
try:
    from langsmith import traceable
except Exception:  # pragma: no cover
    def traceable(func):
        return func
    
from app.langgraph.state.ephemeral_context import State
from app.langgraph.utils.merge_utils import merge_profile, merge_collection
from app.dao.db_user_utils import fetch_profile_from_db, fetch_collections_from_db

load_dotenv()

# -------------------------------------------------------------------
# DB URL
# -------------------------------------------------------------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not configured")

# psycopg3 전용 DSN 형태로 통일
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)


# -------------------------------------------------------------------
# Embedding Model (SentenceTransformer, BGE-m3-ko)
# -------------------------------------------------------------------
_EMBED_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "dragonkue/bge-m3-ko")
_EMBED_DEVICE = os.getenv("EMBEDDING_DEVICE", "cpu")

_embedding_model: Optional[SentenceTransformer] = None


def _get_embed_model() -> SentenceTransformer:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer(_EMBED_MODEL_NAME, device=_EMBED_DEVICE)
    return _embedding_model


def _embed_text(text: str) -> List[float]:
    """
    SentenceTransformer encode → list[float]
    - normalize_embeddings=True 로 cosine distance에 맞춘다.
    """
    model = _get_embed_model()
    return model.encode(text or "", normalize_embeddings=True).tolist()


# -------------------------------------------------------------------
# DB Connection
# -------------------------------------------------------------------
def _get_conn():
    return psycopg.connect(DB_URL)


# -------------------------------------------------------------------
# Keyword Extraction (단순 토큰 추출)
# -------------------------------------------------------------------
def extract_keywords(text: str, max_k: int = 8) -> List[str]:
    """
    쿼리 텍스트에서 한글/영문/숫자 토큰만 뽑고
    자주 쓰이는 불용어를 제거한 뒤 상위 max_k개만 반환.
    """
    if not text:
        return []
    tokens = re.findall(r"[가-힣A-Za-z0-9]+", text)
    stop = {"그리고", "하지만", "근데", "가능", "문의", "신청", "여부", "있나요", "해당"}
    out, seen = [], set()
    for t in tokens:
        t = t.lower()
        if len(t) >= 2 and t not in stop:
            if t not in seen:
                seen.add(t)
                out.append(t)
                if len(out) >= max_k:
                    break
    return out


# -------------------------------------------------------------------
# Region Sanitizer
# -------------------------------------------------------------------
def _sanitize_region(region_value: Optional[Any]) -> Optional[str]:
    """
    region 값을 문자열로 정리.
    - dict 형태({'value': '강남구'})도 지원.
    - 공백/빈 문자열이면 None.
    """
    if region_value is None:
        return None

    if isinstance(region_value, dict):
        region_value = region_value.get("value")

    if region_value is None:
        return None

    region_str = str(region_value).strip()
    return region_str or None


# -------------------------------------------------------------------
# Hybrid Document Search (현재는 Vector + optional region filter)
# -------------------------------------------------------------------
def _hybrid_search_documents(
    query_text: str,
    merged_profile: Optional[Dict[str, Any]],
    top_k: int = 8,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    오직 query_text 임베딩 기반 pgvector 검색.
    (collection predicate/object 필터는 아직 사용하지 않음)

    반환:
      - rag_snippets: 문서 컨텍스트 리스트
      - keywords: 쿼리에서 추출된 키워드 리스트
    """
    query_text = (query_text or "").strip()
    if not query_text:
        return [], []

    keywords = extract_keywords(query_text, max_k=8)

    # region filter: merged_profile 내 residency_sgg_code(or region_gu)을 사용
    region_filter = None
    if merged_profile:
        # 우선 residency_sgg_code, 없으면 region_gu를 사용
        region_val = merged_profile.get("residency_sgg_code")
        if region_val is None:
            region_val = merged_profile.get("region_gu")
        region_filter = _sanitize_region(region_val)
        if region_filter is None:
            # 디버깅용 로그 정도로만 사용 (필수 아님)
            print("[retrieval_planner] region_filter empty or missing")

    # embedding
    try:
        qvec = _embed_text(query_text)
    except Exception as e:
        print(f"[retrieval_planner] embed failed: {e}")
        return [], keywords

    # psycopg3에서 VECTOR 타입으로 캐스팅하기 위해 문자열 리터럴 사용
    qvec_str = "[" + ",".join(f"{v:.6f}" for v in qvec) + "]"

    sql = """
        SELECT
            d.id,
            d.title,
            d.requirements,
            d.benefits,
            d.region,
            d.url,
            1 - (e.embedding <=> %(qvec)s::vector) AS similarity
        FROM documents d
        JOIN embeddings e ON d.id = e.doc_id
    """

    params: Dict[str, Any] = {"qvec": qvec_str}

    if region_filter:
        sql += " WHERE TRIM(d.region) = %(region)s::text"
        params["region"] = region_filter

    sql += """
        ORDER BY e.embedding <=> %(qvec)s::vector
        LIMIT %(limit)s
    """
    params["limit"] = top_k

    rows = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results: List[Dict[str, Any]] = []
    for r in rows:
        similarity = float(r[6]) if r[6] is not None else None
        requirements = (r[2] or "").strip() if isinstance(r[2], str) else None
        benefits = (r[3] or "").strip() if isinstance(r[3], str) else None
        region = (r[4] or "").strip() if isinstance(r[4], str) else None
        url = (r[5] or "").strip() if isinstance(r[5], str) else None

        snippet_lines: List[str] = []
        if requirements:
            snippet_lines.append(f"[신청 요건]\n{requirements}")
        if benefits:
            snippet_lines.append(f"[지원 내용]\n{benefits}")
        snippet_text = "\n\n".join(snippet_lines).strip()

        results.append(
            {
                "doc_id": r[0],
                "title": (r[1] or "").strip() if isinstance(r[1], str) else None,
                "requirements": requirements,
                "benefits": benefits,
                "region": region,
                "url": url,
                "similarity": similarity,
                "snippet": snippet_text,
            }
        )

    # similarity 내림차순 정렬 (SQL에서도 정렬하지만 혹시 모르니)
    results.sort(key=lambda x: (x["similarity"] is not None, x["similarity"]), reverse=True)

    # rag_snippets 포맷으로 재구성
    snippets: List[Dict[str, Any]] = []
    for r in results:
        snippet_entry: Dict[str, Any] = {
            "doc_id": r["doc_id"],
            "title": r["title"],
            "source": r["region"] or "policy_db",
            "snippet": r["snippet"] or r["benefits"] or r["requirements"] or "",
            "score": r["similarity"],
        }
        if r["region"]:
            snippet_entry["region"] = r["region"]
        if r["url"]:
            snippet_entry["url"] = r["url"]
        if r["requirements"]:
            snippet_entry["requirements"] = r["requirements"]
        if r["benefits"]:
            snippet_entry["benefits"] = r["benefits"]
        snippets.append(snippet_entry)

    return snippets, keywords


# -------------------------------------------------------------------
# use_rag 결정 함수
# -------------------------------------------------------------------
def _decide_use_rag(router: Optional[Dict[str, Any]], query_text: str) -> bool:
    """
    router 정보와 쿼리 텍스트를 바탕으로 RAG 사용 여부를 결정.
    - 1순위: router["use_rag"] 값
    - 2순위: category/텍스트 기반 휴리스틱 (필요하면 확장 가능)
    """
    if not router:
        return True  # 기본값: 사용

    if "use_rag" in router:
        return bool(router["use_rag"])

    # fallback 휴리스틱 (지금은 매우 보수적으로 사용)
    text = (query_text or "").lower()
    if any(k in text for k in ["자격", "지원", "혜택", "대상", "요건", "급여", "본인부담"]):
        return True

    return False


# -------------------------------------------------------------------
# Retrieval Planner Node
# -------------------------------------------------------------------
@traceable
def plan(state: State) -> State:
    """
    LangGraph 노드 함수.

    입력 State (중요 필드):
      - profile_id: int | None
      - user_input: str
      - router: {category, save_profile, save_collection, use_rag, ...}
      - ephemeral_profile: 세션 중 추출된 프로필 오버레이
      - ephemeral_collection: 세션 중 추출된 컬렉션(triples)

    동작:
      1) DB에서 profile/collections 읽기 (profile_id 기준)
      2) merge_profile / merge_collection으로 ephemeral과 병합
      3) router/use_rag 플래그를 보고 RAG ON/OFF 결정
      4) 필요 시 문서 검색 → rag_snippets 생성
      5) state["retrieval"]에 저장

    출력:
      state["retrieval"] = {
        "used_rag": bool,
        "profile_ctx": merged_profile or None,
        "collection_ctx": merged_collection or None,
        "rag_snippets": [...],
        "keywords": [...],
      }
    """
    profile_id = state.get("profile_id")
    query_text = state.get("user_input") or ""
    router_info: Dict[str, Any] = state.get("router") or {}

    # --- ephemeral ---
    eph_profile = state.get("ephemeral_profile")
    eph_collection = state.get("ephemeral_collection")

    # --- DB fetch (profile_id 기준) ---
    db_profile = None
    db_collection = None
    if profile_id is not None:
        try:
            db_profile = fetch_profile_from_db(profile_id)
        except Exception as e:
            print(f"[retrieval_planner] fetch_profile_from_db error: {e}")

        try:
            db_collection = fetch_collections_from_db(profile_id)
        except Exception as e:
            print(f"[retrieval_planner] fetch_collections_from_db error: {e}")

    # --- merge ephemeral + DB ---
    merged_profile = merge_profile(db_profile, eph_profile)
    merged_collection = merge_collection(db_collection, eph_collection)

    # --- RAG 사용 여부 결정 ---
    use_rag = _decide_use_rag(router_info, query_text)

    rag_docs: List[Dict[str, Any]] = []
    keywords: List[str] = []

    if use_rag and query_text:
        try:
            rag_docs, keywords = _hybrid_search_documents(
                query_text=query_text,
                merged_profile=merged_profile,
                top_k=8,
            )
        except Exception as e:
            print(f"[retrieval_planner] document search failed: {e}")
            rag_docs = []
            keywords = extract_keywords(query_text, max_k=8)
    else:
        # RAG를 쓰지 않는 경우에도, 쿼리 키워드 정도는 남겨둘 수 있다.
        keywords = extract_keywords(query_text, max_k=8)

    # 대화 저장 안내 스니펫 추가 조건
    end_requested = bool(state.get("end_session"))
    save_keywords = ("저장", "보관", "기록")
    refers_to_save = any(k in query_text for k in save_keywords)
    if end_requested or refers_to_save:
        rag_docs.append({
            "doc_id": "system:conversation_persist",
            "title": "대화 저장 안내",
            "snippet": "대화를 종료하면 저장 파이프라인이 자동 실행되어 대화 내용이 보관됩니다.",
            "score": 1.0,
        })

    state["retrieval"] = {
        "used_rag": use_rag,
        "profile_ctx": merged_profile,
        "collection_ctx": merged_collection,
        "rag_snippets": rag_docs,
        "keywords": keywords,
    }

    state["rag_snippets"] = rag_docs

    return state


# -------------------------------------------------------------------
# Manual Test
# -------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.utcnow().isoformat()


if __name__ == "__main__":
    # 이 테스트는 단독 실행용이며,
    # 실제 LangGraph service_graph.py와는 별도의 샌드박스다.
    dummy_state: State = {  # type: ignore
        "session_id": "sess-test-1",
        "profile_id": 1,  # 실제 DB에 존재하는 profiles.id로 교체해서 테스트 권장
        "user_input": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
        "router": {
            "category": "POLICY_QA",
            "save_profile": True,
            "save_collection": True,
            "use_rag": True,
            "reason": "테스트",
        },
        "ephemeral_profile": {
            "residency_sgg_code": {"value": "강남구", "confidence": 0.95},
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
                    "confidence": 0.95,
                }
            ]
        },
        "messages": [
            {
                "role": "user",
                "content": "재난적의료비 대상인가요? 임신 중이고 유방암 진단을 받았습니다.",
                "created_at": _now_iso(),
                "meta": {},
            }
        ],
    }

    out = retrieval_planner_node(dummy_state)  # type: ignore
    #out = plan(dummy_state)  # type: ignore
    print(json.dumps(out["retrieval"], ensure_ascii=False, indent=2, default=str))
