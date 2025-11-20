# app/langgraph/nodes/policy_retriever_node.py
# -*- coding: utf-8 -*-
"""
policy_retriever_node.py (단순화/재설계 + 컬렉션 계층 + synthetic query 버전)

역할:
  1) user_context_node가 만든 컨텍스트를 읽어 검색용 search_text 구성
     - profile_summary_text
     - merged_collection(질환/치료 요약)
     - 현재 질문(user_input)
  2) "정책 요청 문장" 임베딩 + pgvector 기반 정책 DB 검색 (제목 title만)
     - 단, 질문이 너무 일반적이면 profile/collection 기반 synthetic query 사용
  3) region + 프로필 기반 하드 필터링
  4) 컬렉션 계층(L0/L1/L2) 트리플을 BM25 키워드로 사용
     - L0(이번 턴) > L1(이번 세션) > L2(DB) 순으로 step-weight 가중
  5) hybrid score(벡터 + BM25)로 최종 랭킹
  6) state["retrieval"], state["rag_snippets"], state["context"] 세팅
"""

from __future__ import annotations

import os
import re
import math
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
from app.langgraph.utils.retrieval_filters import filter_candidates_by_profile

load_dotenv()

# -------------------------------------------------------------------
# DB URL
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

# 컬렉션 계층별 weight (L0 > L1 > L2)
LAYER_WEIGHTS = {
    "L0": 3,  # 이번 턴 새로 추출된 triples
    "L1": 2,  # 이번 세션 ephemeral_collection
    "L2": 1,  # DB collections
}


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
# Keyword Extraction
# -------------------------------------------------------------------
def extract_keywords(text: str, max_k: int = 8) -> List[str]:
    """
    쿼리 텍스트에서 한글/영문/숫자 토큰만 뽑고
    자주 쓰이는 불용어를 제거한 뒤 상위 max_k개만 반환.
    """
    if not text:
        return []
    tokens = re.findall(r"[가-힣A-Za-z0-9]+", text)
    stop = {
        "그리고",
        "하지만",
        "근데",
        "혹시",
        "만약",
        "받을",
        "가능",
        "문의",
        "신청",
        "여부",
        "있나요",
        "해당",
        "사용자",
        "상태",
        "현재",
        "질문",
        "혜택",
        "지원",
        "정책",
        "제가",
        "나는",
        "저는",
        "내가",
        "궁금",
        "궁금해요",
    }
    out: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        t = t.lower()
        if len(t) >= 2 and t not in stop:
            if t not in seen:
                seen.add(t)
                out.append(t)
                if len(out) >= max_k:
                    break
    return out


def _parse_created_at(tri: Dict[str, Any]) -> Optional[datetime]:
    """
    triple의 created_at을 datetime으로 파싱.
    - 없거나 파싱 실패하면 None 반환.
    (현재 계층 weight 중심이라 필수는 아님, 호환성용)
    """
    v = tri.get("created_at")
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


# -------------------------------------------------------------------
# BM25 Re-ranking helpers
# -------------------------------------------------------------------
def _tokenize_for_bm25(text: str) -> List[str]:
    """단순 토크나이저: 한글/영문/숫자 토큰을 소문자로 반환."""
    if not text:
        return []
    return [t.lower() for t in re.findall(r"[가-힣A-Za-z0-9]+", text)]


def _add_layer_terms(
    terms: List[str],
    layer: Optional[Dict[str, Any]],
    weight: int,
) -> None:
    """
    특정 컬렉션 레이어의 triples에서 BM25 term들을 추출하여,
    주어진 weight만큼 반복 삽입.
    """
    if not isinstance(layer, dict):
        return
    triples = layer.get("triples") or []
    if not isinstance(triples, list):
        return

    for tri in triples:
        if not isinstance(tri, dict):
            continue
        obj = (tri.get("object") or "").strip()
        code = (tri.get("code") or "").strip()
        if not obj and not code:
            continue
        toks = _tokenize_for_bm25(f"{obj} {code}")
        if not toks:
            continue

        for tok in toks:
            if not tok:
                continue
            # 각 triple term은 weight 번까지 허용
            for _ in range(max(weight, 1)):
                terms.append(tok)


def _build_bm25_terms_from_layers(
    collection_L0: Optional[Dict[str, Any]],
    collection_L1: Optional[Dict[str, Any]],
    collection_L2: Optional[Dict[str, Any]],
) -> List[str]:
    """
    BM25용 쿼리 토큰 구성.

    - 현재 user_query는 여기서 사용하지 않는다 (계층 설계에 맞춰 제거).
    - 컬렉션 계층을 통해 "상태/질환/치료" 키워드만 반영.

    계층 구조:
      L0: 이번 턴에서 새로 추출된 triples (가장 중요)
      L1: 이번 세션 ephemeral_collection
      L2: DB collections (가장 낮은 weight)
    """
    terms: List[str] = []

    _add_layer_terms(terms, collection_L0, LAYER_WEIGHTS.get("L0", 3))
    _add_layer_terms(terms, collection_L1, LAYER_WEIGHTS.get("L1", 2))
    _add_layer_terms(terms, collection_L2, LAYER_WEIGHTS.get("L2", 1))

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
# Region Sanitizer
# -------------------------------------------------------------------
def _sanitize_region(region_value: Optional[Any]) -> Optional[str]:
    """
    region 값을 문자열로 정리.
    - dict 형태({'value': '강남구'})도 지원.
    - '서울시 동작구', '서울특별시 동작구' 등은 마지막 토큰('동작구')만 남김.
    - 공백/빈 문자열이면 None.
    """
    if region_value is None:
        return None

    if isinstance(region_value, dict):
        region_value = region_value.get("value")

    if region_value is None:
        return None

    region_str = str(region_value).strip()
    if not region_str:
        return None

    # 공백 기준으로 자르고, 뒤에서부터 '구/군/동/시'로 끝나는 토큰 찾기
    tokens = region_str.split()
    for tok in reversed(tokens):
        tok = tok.strip()
        if not tok:
            continue
        # '동작구', '분당구', '청주시' 등
        if tok.endswith(("구", "군", "동", "시")):
            return tok

    # 위에서 못 찾으면 원문 그대로 사용
    return region_str


# -------------------------------------------------------------------
# Synthetic Query Builder
# -------------------------------------------------------------------
def _collect_layer_objects(layer: Optional[Dict[str, Any]]) -> List[str]:
    """
    컬렉션 레이어에서 object 텍스트만 모아서 리스트로 반환.
    (predicate는 여기서 구분하지 않고, 질환/치료/에피소드 전부 상태 신호로 사용)
    """
    results: List[str] = []
    if not isinstance(layer, dict):
        return results
    triples = layer.get("triples") or []
    if not isinstance(triples, list):
        return results

    for tri in triples:
        if not isinstance(tri, dict):
            continue
        obj = (tri.get("object") or "").strip()
        if obj:
            results.append(obj)
    return results


def _build_synthetic_query(
    raw_query: str,
    profile_summary_text: Optional[str],
    collection_L0: Optional[Dict[str, Any]],
    collection_L1: Optional[Dict[str, Any]],
) -> str:
    """
    질문이 너무 일반적일 때(title 임베딩에 그대로 쓰면 의미 없는 경우),
    사용자 상태 + 최근 컬렉션 기반으로 synthetic query를 만들어준다.

    규칙:
      - raw_query 에서 의미 있는 키워드가 있으면 그대로 사용.
      - extract_keywords(raw_query)가 비어 있으면 "generic" 으로 보고 synthetic 사용.
    """
    raw_query = (raw_query or "").strip()
    core_kws = extract_keywords(raw_query, max_k=4)

    # 정보가 있는 질문이면 그냥 원문 사용
    if core_kws:
        return raw_query

    pieces: List[str] = []

    if profile_summary_text:
        pieces.append(profile_summary_text.strip())

    # 최근 상태/질환/치료 키워드를 모은다 (L0 > L1 순서)
    objs: List[str] = []
    objs.extend(_collect_layer_objects(collection_L0))
    objs.extend(_collect_layer_objects(collection_L1))

    uniq_objs: List[str] = []
    seen: set[str] = set()
    for o in objs:
        if o not in seen:
            seen.add(o)
            uniq_objs.append(o)
        if len(uniq_objs) >= 5:
            break

    if uniq_objs:
        pieces.append("최근 상황: " + ", ".join(uniq_objs))

    # 상태/컬렉션 둘 다 비어있으면 fallback으로 raw_query 사용
    if not pieces:
        return raw_query

    pieces.append("관련 의료·복지 지원 정책")
    synthetic = " ".join(pieces)
    print(f"[policy_retriever_node] synthetic query used instead of raw: {synthetic}")
    return synthetic


# -------------------------------------------------------------------
# Hybrid Document Search (제목 title 임베딩만 사용)
# -------------------------------------------------------------------
def _hybrid_search_documents(
    query_text: str,
    merged_profile: Optional[Dict[str, Any]],
    top_k: int = 8,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    query_text 임베딩 기반 pgvector 검색.
    - query_text: 오직 "정책 요청용" 텍스트 (raw 또는 synthetic)만 사용
    - title 임베딩만 사용해서 정책 제목과의 유사도 측정
    - region 은 DB 레벨 하드 필터링
    """
    query_text = (query_text or "").strip()
    if not query_text:
        return [], []

    # 키워드 추출 (로그용)
    debug_keywords = extract_keywords(query_text, max_k=8)

    # 1) region filter
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

    # 2) 임베딩 계산 (정책 요청용 텍스트)
    try:
        qvec = _embed_text(query_text)
    except Exception as e:
        print(f"[policy_retriever_node] embed failed: {e}")
        return [], debug_keywords

    # psycopg3에서 VECTOR 타입으로 캐스팅하기 위해 문자열 리터럴 사용
    qvec_str = "[" + ",".join(f"{v:.6f}" for v in qvec) + "]"

    # 3) pgvector 검색 + (선택적) 지역 하드필터
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
        JOIN embeddings e
          ON d.id = e.doc_id
         AND e.field = 'title'
    """
    params = {"qvec": qvec_str}

    if region_filter:
        # '동작구'이면 '서울시 동작구', '동작구' 둘 다 매칭
        sql += " WHERE d.region ILIKE %(region)s"
        params["region"] = f"%{region_filter}%"

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

    # 4) 결과 가공 → rag_snippets 포맷
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

    # similarity 내림차순 정렬 (SQL에서도 정렬하지만 혹시 몰라 한 번 더)
    results.sort(
        key=lambda x: (x["similarity"] is not None, x["similarity"]), reverse=True
    )

    # rag_snippets 포맷으로 재구성
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

    return snippets, debug_keywords


# -------------------------------------------------------------------
# use_rag 결정 함수
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
    if any(
        k in text for k in ["자격", "지원", "혜택", "대상", "요건", "급여", "본인부담"]
    ):
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
          * collection_layer_L0 / L1 / L2
          * profile_summary_text
          * history_text
          * rolling_summary
      - router: dict (use_rag 등)
      - user_input: str (현재 질문)  ← info_extractor가 정보부분 제거한 "정책 요청 문장"일 수 있음

    출력/갱신:
      - state["retrieval"], state["rag_snippets"], state["context"]
    """
    # raw user query (정보 제거 후 정책 요청 문장일 수 있음)
    query_text = state.get("user_input") or ""
    router_info: Dict[str, Any] = state.get("router") or {}

    merged_profile: Optional[Dict[str, Any]] = state.get("merged_profile")
    merged_collection: Optional[Dict[str, Any]] = state.get("merged_collection")
    profile_summary_text: Optional[str] = state.get("profile_summary_text")
    history_text: Optional[str] = state.get("history_text")
    rolling_summary: Optional[str] = state.get("rolling_summary")

    # 컬렉션 계층 레이어
    collection_L0: Optional[Dict[str, Any]] = state.get("collection_layer_L0")
    collection_L1: Optional[Dict[str, Any]] = state.get("collection_layer_L1")
    collection_L2: Optional[Dict[str, Any]] = state.get("collection_layer_L2")

    # 1) search_text (로그/디버깅용) - 임베딩에는 사용 안 함
    search_parts: List[str] = []

    if profile_summary_text:
        search_parts.append(profile_summary_text)

    # 컬렉션의 질환/치료를 문장으로 풀어주기 (정보 제공용)
    if merged_collection and isinstance(merged_collection, dict):
        diseases: List[str] = []
        treatments: List[str] = []
        for tri in merged_collection.get("triples") or []:
            if tri.get("predicate") == "disease":
                diseases.append(tri.get("object"))
            elif tri.get("predicate") == "treatment":
                treatments.append(tri.get("object"))
        extra_lines: List[str] = []
        if diseases:
            extra_lines.append("주요 질환: " + ", ".join(diseases))
        if treatments:
            extra_lines.append("주요 치료: " + ", ".join(treatments))
        if extra_lines:
            search_parts.append("\n".join(extra_lines))

    if history_text:
        # 이전 대화 텍스트는 검색에 쓰지는 않지만, 요약/설명용으로만 포함 가능
        search_parts.append("최근 대화 요약:\n" + history_text.strip())

    if query_text:
        search_parts.append("현재 질문: " + query_text.strip())

    search_text = "\n\n".join(search_parts).strip() if search_parts else query_text

    # --- RAG 사용 여부 결정 (raw query 기준) ---
    use_rag = _decide_use_rag(router_info, query_text)

    rag_docs: List[Dict[str, Any]] = []
    debug_keywords: List[str] = []

    if use_rag and query_text.strip():
        try:
            # 1) synthetic 여부 판단 + 정책용 embedding query 생성
            embedding_query = _build_synthetic_query(
                raw_query=query_text,
                profile_summary_text=profile_summary_text,
                collection_L0=collection_L0,
                collection_L1=collection_L1,
            )

            # 2) 검색에는 embedding_query만 사용
            rag_docs, debug_keywords = _hybrid_search_documents(
                query_text=embedding_query,
                merged_profile=merged_profile,
                top_k=RAW_TOP_K,
            )
        except Exception as e:  # noqa: E722
            print(f"[policy_retriever_node] document search failed: {e}")
            rag_docs = []
            debug_keywords = extract_keywords(query_text, max_k=8)
    else:
        debug_keywords = extract_keywords(query_text, max_k=8)

    # --- 프로필 기반 후보 필터 적용 (중위소득/기초수급/장애 등 hard filter 역할) ---
    if merged_profile and rag_docs:
        before = len(rag_docs)
        rag_docs = filter_candidates_by_profile(rag_docs, merged_profile)
        after = len(rag_docs)
        print(f"[policy_retriever_node] profile filter: {before} -> {after} candidates")

    bm25_terms: List[str] = []

    # --- similarity 기반 소프트 컷오프 (최소 개수 보장) + BM25 re-ranking ---
    if rag_docs:

        def _get_sim(d: Dict[str, Any]) -> Optional[float]:
            v = d.get("similarity")
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        sims = [s for s in (_get_sim(d) for d in rag_docs) if s is not None]
        if sims:
            filtered_by_sim = [
                d for d in rag_docs if (_get_sim(d) or 0.0) >= SIMILARITY_FLOOR
            ]
            if len(filtered_by_sim) >= MIN_CANDIDATES_AFTER_FLOOR:
                print(
                    f"[policy_retriever_node] similarity floor {SIMILARITY_FLOOR}: "
                    f"{len(rag_docs)} -> {len(filtered_by_sim)} candidates"
                )
                rag_docs = filtered_by_sim

        # --- BM25 기반 re-ranking (컬렉션 계층 기반) ---
        bm25_terms = _build_bm25_terms_from_layers(
            collection_L0,
            collection_L1,
            collection_L2,
        )
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

    # --- retrieval.keywords 구성 ---
    # 1) 사용자 raw query에서 온 키워드
    user_kw = extract_keywords(query_text, max_k=8)
    # 2) BM25 terms 와 합쳐서 중복 제거
    final_keywords: List[str] = []
    seen_kw: set[str] = set()
    for t in user_kw + bm25_terms:
        if t not in seen_kw:
            seen_kw.add(t)
            final_keywords.append(t)
            if len(final_keywords) >= 12:
                break

    # --- retrieval 세팅 ---
    retrieval: Dict[str, Any] = {
        "used_rag": use_rag,
        "profile_ctx": merged_profile,
        "collection_ctx": merged_collection,
        "rag_snippets": rag_docs,
        "keywords": final_keywords,
        "search_text": search_text,  # 디버깅/로그용 전체 텍스트
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
