# app/langgraph/nodes/policy_retriever_node.py
# -*- coding: utf-8 -*-
"""
policy_retriever_node.py

역할:
  1) user_context_node가 만든 컨텍스트를 읽어 검색용 search_text 구성
     - rolling_summary
     - history_text (최근 대화)
     - profile_summary_text
     - 현재 질문(user_input)
  2) query_text 임베딩 + pgvector 기반 정책 DB 검색 (RAG)
  3) 프로필 기반 후보 필터링, system 스니펫 추가
  4) state["retrieval"], state["rag_snippets"], state["context"] 세팅

※ 기존 코드 출처
  - retrieval_planner.py
      * Embedding / DB / 검색 로직 전부
"""

from __future__ import annotations

import os
import re
import math
from typing import Any, Dict, List, Optional, Tuple

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
from app.langgraph.utils.retrieval_filters import filter_candidates_by_profile

load_dotenv()

# -------------------------------------------------------------------
# DB URL (retrieval_planner.py 그대로)
# -------------------------------------------------------------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not configured")

if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

# -------------------------------------------------------------------
# Retriever tunable parameters
# -------------------------------------------------------------------
RAW_TOP_K = int(os.getenv("POLICY_RETRIEVER_RAW_TOP_K", "24"))
CONTEXT_TOP_K = int(os.getenv("POLICY_RETRIEVER_CONTEXT_TOP_K", "24"))
SIMILARITY_FLOOR = float(os.getenv("POLICY_RETRIEVER_SIM_FLOOR", "0.3"))
MIN_CANDIDATES_AFTER_FLOOR = int(os.getenv("POLICY_RETRIEVER_MIN_AFTER_FLOOR", "5"))
BM25_WEIGHT = float(os.getenv("POLICY_RETRIEVER_BM25_WEIGHT", "0.35"))

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
# Keyword Extraction (retrieval_planner.extract_keywords 그대로)
# -------------------------------------------------------------------
def extract_keywords(text: str, max_k: int = 8) -> List[str]:
    """
    쿼리 텍스트에서 한글/영문/숫자 토큰만 뽑고
    자주 쓰이는 불용어를 제거한 뒤 상위 max_k개만 반환.
    """
    if not text:
        return []
    tokens = re.findall(r"[가-힣A-Za-z0-9]+", text)
    stop = {"그리고", "하지만", "근데", "가능", "문의", "신청", "여부", "있나요", "해당", "사용자", "상태", "현재", "질문"}
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
# BM25 Re-ranking helpers
# -------------------------------------------------------------------
def _tokenize_for_bm25(text: str) -> List[str]:
    """단순 토크나이저: 한글/영문/숫자 토큰을 소문자로 반환."""
    if not text:
        return []
    return [t.lower() for t in re.findall(r"[가-힣A-Za-z0-9]+", text)]


def _build_bm25_terms(query_text: str, merged_collection: Optional[Dict[str, Any]]) -> List[str]:
    """사용자 질문 + 컬렉션(질환/병력 등) 기반 BM25 쿼리 토큰 구성."""
    terms: List[str] = []
    # 1) 질문에서 키워드 추출
    terms.extend(extract_keywords(query_text or "", max_k=8))

    # 2) 컬렉션 triple에서 object/code에 포함된 키워드 추가 (중복 제거)
    if merged_collection and isinstance(merged_collection, dict):
        triples = merged_collection.get("triples") or []
        for tri in triples:
            if not isinstance(tri, dict):
                continue
            obj = tri.get("object") or ""
            code = tri.get("code") or ""
            for tok in _tokenize_for_bm25(str(obj) + " " + str(code)):
                if tok not in terms:
                    terms.append(tok)
    return terms


def _apply_bm25_rerank(
    docs: List[Dict[str, Any]],
    query_terms: List[str],
) -> None:
    """주어진 후보 docs에 대해 BM25 점수를 계산하고 score 필드를 hybrid 점수로 갱신."""
    if not docs or not query_terms:
        return

    # 문서별 토큰/길이/term frequency 계산
    doc_tokens: List[List[str]] = []
    doc_lens: List[int] = []
    term_doc_freq: Dict[str, int] = {t: 0 for t in query_terms}

    for doc in docs:
        text_parts = [
            doc.get("title") or "",
            doc.get("requirements") or "",
            doc.get("benefits") or "",
        ]
        tokens = _tokenize_for_bm25(" ".join(text_parts))
        doc_tokens.append(tokens)
        dl = len(tokens) or 1
        doc_lens.append(dl)

        # 각 쿼리 term이 등장하는지 세기
        token_set = set(tokens)
        for t in query_terms:
            if t in token_set:
                term_doc_freq[t] = term_doc_freq.get(t, 0) + 1

    N = len(docs)
    avgdl = sum(doc_lens) / float(N)

    # BM25 파라미터
    k1 = 1.5
    b = 0.75

    bm25_scores: List[float] = []
    for idx, tokens in enumerate(doc_tokens):
        tf: Dict[str, int] = {}
        for tok in tokens:
            if tok in query_terms:
                tf[tok] = tf.get(tok, 0) + 1

        dl = doc_lens[idx]
        score = 0.0
        for term in query_terms:
            n_qi = term_doc_freq.get(term, 0)
            if n_qi == 0:
                continue
            # BM25 idf
            idf = math.log((N - n_qi + 0.5) / (n_qi + 0.5) + 1)
            freq = tf.get(term, 0)
            if freq == 0:
                continue
            denom = freq + k1 * (1 - b + b * dl / avgdl)
            score += idf * (freq * (k1 + 1)) / denom
        bm25_scores.append(score)

    max_bm25 = max(bm25_scores) if bm25_scores else 0.0

    # hybrid 점수 계산: similarity(벡터) + BM25
    for doc, bm25 in zip(docs, bm25_scores):
        # raw similarity는 별도 필드로 유지
        sim_val = doc.get("similarity")
        try:
            sim = float(sim_val) if sim_val is not None else 0.0
        except (TypeError, ValueError):
            sim = 0.0

        bm25_norm = (bm25 / max_bm25) if max_bm25 > 0 else 0.0
        hybrid = (1.0 - BM25_WEIGHT) * sim + BM25_WEIGHT * bm25_norm

        doc["bm25_score"] = bm25
        # LLM/후속 단계에서 사용할 최종 score를 hybrid로 덮어씀
        doc["score"] = hybrid


# -------------------------------------------------------------------
# Region Sanitizer (retrieval_planner._sanitize_region 그대로)
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
# Hybrid Document Search (retrieval_planner._hybrid_search_documents 그대로)
# -------------------------------------------------------------------
def _hybrid_search_documents(
    query_text: str,
    merged_profile: Optional[Dict[str, Any]],
    top_k: int = 8,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    query_text 임베딩 기반 pgvector 검색.
    query_text 안에는 이미 "요약 + 최근 대화 + 사용자 상태 요약 + 현재 질문"이 포함되어 있음.
    """
    query_text = (query_text or "").strip()
    if not query_text:
        return [], []

    # 키워드 추출 (디버깅/로그용)
    keywords = extract_keywords(query_text, max_k=8)

    # ─────────────────────────────────────────────
    # 1) region filter: merged_profile 내 residency_sgg_code(or region_gu)을 사용
    #    → retrieval_planner와 동일한 하드필터링
    # ─────────────────────────────────────────────
    region_filter: Optional[str] = None
    if merged_profile:
        region_val = merged_profile.get("residency_sgg_code")
        if region_val is None:
            region_val = merged_profile.get("region_gu")
        print("[policy_retriever_node] merged_profile region_raw:", region_val)
        region_filter = _sanitize_region(region_val)
        print("[policy_retriever_node] region_filter after sanitize:", region_filter)
        if region_filter is None:
            print("[policy_retriever_node] region_filter empty or missing")
    else:
        print("[policy_retriever_node] merged_profile is None or empty")

    # ─────────────────────────────────────────────
    # 2) 임베딩 계산
    # ─────────────────────────────────────────────
    try:
        qvec = _embed_text(query_text)
    except Exception as e:
        print(f"[policy_retriever_node] embed failed: {e}")
        return [], keywords

    # psycopg3에서 VECTOR 타입으로 캐스팅하기 위해 문자열 리터럴 사용
    qvec_str = "[" + ",".join(f"{v:.6f}" for v in qvec) + "]"

    # ─────────────────────────────────────────────
    # 3) pgvector 검색 + (선택적) 지역 하드필터
    # ─────────────────────────────────────────────
    sql = """
        SELECT
            d.id,
            d.title,
            d.requirements,
            d.benefits,
            d.region,
            d.url,
            MAX(1 - (e.embedding <=> %(qvec)s::vector)) AS similarity
        FROM documents d
        JOIN embeddings e ON d.id = e.doc_id AND e.field = 'requirements'
    """
    params = {"qvec": qvec_str}

    if region_filter:
        sql += " WHERE TRIM(d.region) = %(region)s::text"
        params["region"] = region_filter

    sql += """
        GROUP BY
            d.id, d.title, d.requirements, d.benefits, d.region, d.url
        ORDER BY similarity DESC
        LIMIT %(limit)s
    """
    params["limit"] = top_k


    rows = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    # ─────────────────────────────────────────────
    # 4) 결과 가공 → rag_snippets 포맷
    # ─────────────────────────────────────────────
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

    snippets: List[Dict[str, Any]] = []
    for r in results:
        snippet_entry: Dict[str, Any] = {
            "doc_id": r["doc_id"],
            "title": r["title"],
            "source": r["region"] or "policy_db",
            "snippet": r["snippet"] or r["benefits"] or r["requirements"] or "",
            # 초기 score는 벡터 유사도와 동일하게 설정
            "similarity": r["similarity"],
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
# use_rag 결정 함수 (retrieval_planner._decide_use_rag 그대로)
# -------------------------------------------------------------------
def _decide_use_rag(router: Optional[Dict[str, Any]], query_text: str) -> bool:
    """
    router 정보와 쿼리 텍스트를 바탕으로 RAG 사용 여부를 결정.
    - 1순위: router["use_rag"] 값
    - 2순위: category/텍스트 기반 휴리스틱
    """
    if not router:
        return True

    if "use_rag" in router:
        return bool(router["use_rag"])

    text = (query_text or "").lower()
    if any(k in text for k in ["자격", "지원", "혜택", "대상", "요건", "급여", "본인부담"]):
        return True

    return False


# -------------------------------------------------------------------
# 메인 노드 함수
# -------------------------------------------------------------------
@traceable
def policy_retriever_node(state: State) -> State:
    """
    LangGraph 노드:

    입력:
      - user_context_node에서 채운 값들:
          * merged_profile / merged_collection
          * profile_summary_text
          * history_text
          * rolling_summary
      - router: dict (use_rag 등)
      - user_input: str (현재 질문)

    출력/갱신:
      - state["retrieval"], state["rag_snippets"], state["context"]
    """
    query_text = state.get("user_input") or ""
    router_info: Dict[str, Any] = state.get("router") or {}

    merged_profile: Optional[Dict[str, Any]] = state.get("merged_profile")
    merged_collection: Optional[Dict[str, Any]] = state.get("merged_collection")
    profile_summary_text: Optional[str] = state.get("profile_summary_text")
    history_text: Optional[str] = state.get("history_text")
    rolling_summary: Optional[str] = state.get("rolling_summary")

    # --- 검색용 search_text 구성 ---
    search_parts: List[str] = []
    if rolling_summary:
        search_parts.append("[이전 요약]\n" + rolling_summary)
    if history_text:
        search_parts.append("[최근 대화]\n" + history_text)
    if profile_summary_text:
        search_parts.append(profile_summary_text)
    if query_text:
        search_parts.append("현재 질문: " + query_text.strip())

    search_text = "\n\n".join(search_parts).strip() if search_parts else query_text

    # --- RAG 사용 여부 결정 ---
    use_rag = _decide_use_rag(router_info, query_text)

    rag_docs: List[Dict[str, Any]] = []
    keywords: List[str] = []

    if use_rag and search_text:
        try:
            rag_docs, keywords = _hybrid_search_documents(
                query_text=search_text,
                merged_profile=merged_profile,
                top_k=RAW_TOP_K,
            )
        except Exception as e:  # noqa: E722
            print(f"[policy_retriever_node] document search failed: {e}")
            rag_docs = []
            keywords = extract_keywords(search_text, max_k=8)
    else:
        keywords = extract_keywords(search_text or query_text, max_k=8)

    # --- 프로필 기반 후보 필터 적용 ---
    if merged_profile and rag_docs:
        before = len(rag_docs)
        rag_docs = filter_candidates_by_profile(rag_docs, merged_profile)
        after = len(rag_docs)
        print(f"[policy_retriever_node] profile filter: {before} -> {after} candidates")

    # --- similarity 기반 소프트 컷오프 (최소 개수 보장) ---
    if rag_docs:
        def _get_sim(d: Dict[str, Any]) -> Optional[float]:
            v = d.get("similarity")
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        sims = [s for s in (_get_sim(d) for d in rag_docs) if s is not None]
        if sims:
            filtered_by_sim = [d for d in rag_docs if (_get_sim(d) or 0.0) >= SIMILARITY_FLOOR]
            if len(filtered_by_sim) >= MIN_CANDIDATES_AFTER_FLOOR:
                print(
                    f"[policy_retriever_node] similarity floor {SIMILARITY_FLOOR}: "
                    f"{len(rag_docs)} -> {len(filtered_by_sim)} candidates"
                )
                rag_docs = filtered_by_sim

        # --- BM25 기반 re-ranking (질문 + 컬렉션 조건 중심) ---
        bm25_terms = _build_bm25_terms(query_text, merged_collection)
        if bm25_terms:
            print(f"[policy_retriever_node] BM25 re-ranking with terms: {bm25_terms}")
            _apply_bm25_rerank(rag_docs, bm25_terms)

        # hybrid score(벡터+BM25)를 기준으로 정렬 (None은 뒤로)
        def _get_score(d: Dict[str, Any]) -> Optional[float]:
            v = d.get("score")
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        rag_docs.sort(
            key=lambda d: (
                _get_score(d) is None,
                -(_get_score(d) or 0.0),
            )
        )

        # LLM에 넘길 최대 컨텍스트 개수 제한
        if len(rag_docs) > CONTEXT_TOP_K:
            print(
                f"[policy_retriever_node] context_top_k cap {CONTEXT_TOP_K}: "
                f"{len(rag_docs)} -> {CONTEXT_TOP_K} candidates"
            )
            rag_docs = rag_docs[:CONTEXT_TOP_K]

    # --- 대화 저장 안내 스니펫 추가 ---
    end_requested = bool(state.get("end_session"))
    save_keywords = ("저장", "보관", "기록")
    refers_to_save = any(k in query_text for k in save_keywords)
    if end_requested or refers_to_save:
        rag_docs.append(
            {
                "doc_id": "system:conversation_persist",
                "title": "대화 저장 안내",
                "snippet": "대화를 종료하면 저장 파이프라인이 자동 실행되어 대화 내용이 보관됩니다.",
                "score": 1.0,
            }
        )

    # --- retrieval 세팅 ---
    retrieval: Dict[str, Any] = {
        "used_rag": use_rag,
        "profile_ctx": merged_profile,
        "collection_ctx": merged_collection,
        "rag_snippets": rag_docs,
        "keywords": keywords,
        "debug_search_text": search_text,
        "profile_summary_text": profile_summary_text,
    }
    state["retrieval"] = retrieval
    state["rag_snippets"] = rag_docs

    # answer_llm이 바로 쓸 수 있는 context 블록
    state["context"] = {
        "profile": merged_profile,
        "collection": merged_collection,
        "documents": rag_docs,
        "summary": rolling_summary,
    }

    return state
