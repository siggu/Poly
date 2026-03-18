# Poly 프로젝트 면접 완벽 가이드

> **Poly**: 의료 복지 정책 통합정보 제공 AI 챗봇 서비스

---

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [기술 스택](#2-기술-스택)
3. [아키텍처](#3-아키텍처)
4. [AI/ML 스택 심층 분석](#4-aiml-스택-심층-분석)
5. [RAG 시스템 상세](#5-rag-시스템-상세)
6. [LangGraph 파이프라인](#6-langgraph-파이프라인)
7. [핵심 모듈 분석](#7-핵심-모듈-분석)
8. [데이터베이스 설계](#8-데이터베이스-설계)
9. [성능 최적화](#9-성능-최적화)
10. [디자인 패턴](#10-디자인-패턴)
11. [면접 예상 질문 & 답변](#11-면접-예상-질문--답변)
12. [핵심 숫자 요약](#12-핵심-숫자-요약)

---

## 1. 프로젝트 개요

### 1.1 한 줄 소개

> **"Poly는 의료 복지 정책 통합정보 제공 AI 챗봇으로, 사용자 프로필(나이, 지역, 건강보험 등)을 기반으로 맞춤형 정책을 RAG 기반으로 추천하는 서비스입니다."**

### 1.2 해결하려는 문제

| 문제 | 해결 방법 |
|------|----------|
| 정책 정보가 여러 홈페이지에 분산 | 크롤링으로 통합 DB 구축 |
| 능동적 정보 수집 어려움 | AI 챗봇으로 대화형 검색 |
| 지원자격 수동 비교 필요 | 프로필 기반 자동 필터링 |

### 1.3 핵심 기능

- **맞춤형 정책 추천**: 사용자 프로필 기반 RAG 검색
- **멀티 프로필 지원**: 한 계정에서 여러 가족 구성원 관리
- **실시간 스트리밍**: 토큰 단위 응답으로 빠른 체감 속도
- **정보 자동 추출**: 대화에서 프로필/병력 정보 자동 파악

---

## 2. 기술 스택

### 2.1 전체 스택 요약

```
Frontend:  Streamlit
Backend:   FastAPI + Uvicorn
AI/ML:     LangGraph + LangChain + OpenAI GPT-4o-mini
임베딩:    OpenAI text-embedding-3-small (1536차원)
검색:      PGVector (벡터) + BM25 (키워드) 하이브리드
DB:        PostgreSQL + SQLite (체크포인트)
인증:      JWT (Access + Refresh Token)
```

### 2.2 상세 의존성

| 계층 | 기술 | 버전 | 용도 |
|------|------|------|------|
| **백엔드** | FastAPI | 0.108.0 | REST API 서버 |
| **웹 서버** | Uvicorn | 0.34.0 | ASGI 서버 |
| **프론트엔드** | Streamlit | >=1.28.0 | 웹 UI |
| **LLM 프레임워크** | LangChain | 0.3.27 | LLM 오케스트레이션 |
| **워크플로우** | LangGraph | 1.0.1 | 상태 머신 기반 파이프라인 |
| **LLM** | OpenAI GPT-4o-mini | - | 응답 생성, 정보 추출 |
| **임베딩** | text-embedding-3-small | - | 1536차원 벡터 |
| **벡터 DB** | PGVector | 0.3.6 | 임베딩 저장/검색 |
| **키워드 검색** | rank-bm25 | 0.2.2 | BM25 re-ranking |
| **DB** | PostgreSQL | - | 메인 데이터베이스 |
| **체크포인트** | SQLite | - | LangGraph 상태 저장 |
| **인증** | python-jose | 3.3.0 | JWT 토큰 |
| **비밀번호** | bcrypt | 4.0.1 | 해싱 |

---

## 3. 아키텍처

### 3.1 전체 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                     사용자 (웹 브라우저)                          │
└────────────────────────────┬──────────────────────────────────────┘
                             │
                    HTTP/WebSocket
                             │
        ┌────────────────────┴────────────────────┐
        │                                         │
   ┌────▼──────────────────┐         ┌──────────▼────────────┐
   │  프론트엔드            │         │  백엔드               │
   │  (Streamlit)          │         │  (FastAPI)           │
   │  Port: 8501           │         │  Port: 8000          │
   │                       │         │                      │
   │  ├─ login.py         │         │  ├─ user API        │
   │  ├─ chat.py          │◄────────►  ├─ chat API        │
   │  ├─ my_page.py       │  REST API  ├─ LangGraph      │
   │  └─ settings.py      │         │  └─ Auth            │
   └───────────────────────┘         └──────────┬───────────┘
                                                │
                                     ┌──────────┴──────────┐
                                     │                     │
                               ┌─────▼─────┐         ┌────▼────┐
                               │ PostgreSQL│         │  SQLite │
                               │ + PGVector│         │Checkpoint│
                               └───────────┘         └─────────┘
```

### 3.2 요청-응답 흐름

```
사용자 입력 → Streamlit → FastAPI → LangGraph 파이프라인
                                         │
                                         ├→ session_orchestrator
                                         ├→ query_router
                                         ├→ info_extractor
                                         ├→ user_context_node
                                         ├→ policy_retriever (RAG)
                                         ├→ llm_answer_creator
                                         └→ persist_pipeline
```

---

## 4. AI/ML 스택 심층 분석

### 4.1 AI Agent 아키텍처

> **"Agent는 LLM을 두뇌로 사용하여 자율적으로 작업을 수행하는 시스템입니다."**

Poly의 Agent는 **LangGraph 기반 상태 머신**으로 구현:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        LangGraph Agent Pipeline                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   사용자 입력                                                             │
│       │                                                                  │
│       ▼                                                                  │
│   ┌─────────────────────┐                                                │
│   │ session_orchestrator │  ← 세션 생명주기 관리 (타임아웃, 턴 카운트)     │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │    query_router     │  ← 의도 분류 (save/reset/normal)               │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │   info_extractor    │  ← LLM으로 정보 추출 (NER + 구조화)            │
│   │   (GPT-4o-mini)     │                                                │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │  user_context_node  │  ← 프로필/컬렉션 병합 + 요약 텍스트 생성       │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │  policy_retriever   │  ← RAG: 벡터 + BM25 하이브리드 검색           │
│   │  (OpenAI Embedding) │                                                │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │  llm_answer_creator │  ← 최종 응답 생성 (GPT-4o-mini, 스트리밍)      │
│   └──────────┬──────────┘                                                │
│              │                                                           │
│              ▼                                                           │
│   ┌─────────────────────┐                                                │
│   │   persist_pipeline  │  ← (선택) 대화 히스토리 DB 저장                │
│   └─────────────────────┘                                                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 왜 LangGraph인가?

| 기능 | LangChain만 | LangGraph |
|------|------------|-----------|
| 순차 체인 | O | O |
| 조건부 분기 | 제한적 | **O (conditional_edges)** |
| 상태 공유 | 수동 관리 | **State 자동 관리** |
| 사이클/루프 | X | **O** |
| 체크포인트 | X | **O (SqliteSaver)** |
| 멀티턴 대화 | 복잡 | **간단** |

### 4.3 State 관리 (EphemeralContextState)

```python
class EphemeralContextState(TypedDict, total=False):
    # ── 세션/제어 ───────────────────────────────────
    session_id: str                # 세션 식별자
    end_session: bool              # 세션 종료 플래그
    turn_count: int                # 대화 턴 수

    # ── 메시지 (append-only reducer!) ───────────────
    messages: Annotated[List[Message], operator.add]
    rolling_summary: Optional[str]  # 대화 요약

    # ── 프로필/컬렉션 (계층 구조) ────────────────────
    ephemeral_profile: Dict[str, Any]      # 이번 세션 임시 프로필
    ephemeral_collection: Dict[str, Any]   # 이번 세션 임시 컬렉션
    merged_profile: Dict[str, Any]         # DB + ephemeral 병합
    merged_collection: Dict[str, Any]      # DB + ephemeral 병합

    # ── RAG 결과 ────────────────────────────────────
    retrieval: Dict[str, Any]  # {used_rag, rag_snippets, keywords, ...}

    # ── 스트리밍 ────────────────────────────────────
    streaming_mode: bool
    streaming_context: Dict[str, Any]
```

### 4.4 Reducer 패턴 (append-only messages)

```python
# operator.add를 사용한 Annotated 타입
messages: Annotated[List[Message], operator.add]

# 각 노드는 새 메시지만 반환 → 자동으로 기존에 append
def info_extractor_node(state: State) -> Dict[str, Any]:
    return {
        "messages": [new_message],  # 이것만 반환
        "ephemeral_profile": merged_profile,
    }
```

---

## 5. RAG 시스템 상세

### 5.1 RAG 파이프라인 전체 흐름

```
사용자 질문: "저는 동작구에 사는 당뇨 환자인데 받을 수 있는 혜택이 뭔가요?"
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 1: Synthetic Query 생성                                            │
│                                                                          │
│ • 질문이 너무 일반적인지 판단 (키워드 추출)                              │
│ • 일반적이면 프로필 + 컬렉션 기반 synthetic query 생성                   │
│                                                                          │
│ 결과: "동작구 거주; 당뇨병 관련 의료·복지 지원 정책"                      │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 2: 임베딩 생성 (OpenAI text-embedding-3-small)                     │
│                                                                          │
│ • 모델: text-embedding-3-small (1536차원)                               │
│ • 캐싱: MD5 해시 키 + FIFO 방식 (최대 100개)                            │
│ • 빈 텍스트: 제로 벡터 [0.0] * 1536                                     │
│                                                                          │
│ 성능:                                                                    │
│   - 캐시 히트: ~0.001s                                                  │
│   - 캐시 미스: ~0.8s                                                    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 3: PGVector 벡터 검색                                               │
│                                                                          │
│ SQL:                                                                     │
│   SELECT d.*, (1 - (e.embedding <=> query_vec)) AS similarity           │
│   FROM embeddings e JOIN documents d ON d.id = e.doc_id                 │
│   WHERE e.field = 'title' AND d.region = '동작구'                        │
│   ORDER BY e.embedding <=> query_vec                                    │
│   LIMIT 12  (RAW_TOP_K)                                                 │
│                                                                          │
│ • <=> 연산자: 코사인 거리 (PGVector)                                    │
│ • region 필터: 하드 필터 (인덱스 활용)                                  │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 4: BM25 키워드 Re-ranking                                           │
│                                                                          │
│ 컬렉션 계층별 키워드 가중치:                                             │
│   L0 (이번 턴 추출): weight = 3                                         │
│   L1 (이번 세션):    weight = 2                                         │
│   L2 (DB 저장):      weight = 1                                         │
│                                                                          │
│ BM25 스코어 계산:                                                        │
│   score = Σ IDF(qi) × (freq × (k1 + 1)) / (freq + k1 × (1-b+b×dl/avgdl))│
│   k1 = 1.5, b = 0.75                                                    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 5: 하이브리드 스코어 계산                                           │
│                                                                          │
│   final_score = 0.8 × vector_similarity + 0.2 × bm25_normalized         │
│                                                                          │
│ 비율 선택 이유:                                                          │
│   - 의료 정책은 의미적 유사성이 더 중요 (80%)                           │
│   - 하지만 "당뇨", "임산부" 등 정확한 키워드 매칭도 필요 (20%)          │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 6: 프로필 기반 후처리 필터링                                        │
│                                                                          │
│ 필터 종류:                                                               │
│   1) 중위소득 필터: "중위소득 120% 이하" 조건 파싱 → 사용자 150%면 제외 │
│   2) 기초생활보장 필터: 수급자 필수 정책 → 비수급자 제외                │
│   3) 장애등급 필터: "장애 1~3급" 조건 → 등급 외 제외                    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 7: 최종 Top-K 선택                                                  │
│                                                                          │
│ • SIMILARITY_FLOOR = 0.3 (유사도 임계값)                                 │
│ • MIN_CANDIDATES_AFTER_FLOOR = 5 (최소 후보 수 보장)                    │
│ • CONTEXT_TOP_K = 8 (LLM에 전달할 최대 문서 수)                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 임베딩 캐싱 구현

```python
# app/langgraph/nodes/policy_retriever.py

_embedding_cache: Dict[str, List[float]] = {}  # 캐시 저장소
_cache_order: List[str] = []  # FIFO 순서 관리
EMBEDDING_CACHE_SIZE = 100

def _embed_text(text: str) -> List[float]:
    # 1. 캐시 키 생성 (MD5 해시)
    cache_key = hashlib.md5(text.encode('utf-8')).hexdigest()

    # 2. 캐시 히트
    if cache_key in _embedding_cache:
        return _embedding_cache[cache_key]

    # 3. 캐시 미스 → OpenAI API 호출
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    embedding = response.data[0].embedding

    # 4. 캐시 저장 (FIFO)
    _embedding_cache[cache_key] = embedding
    _cache_order.append(cache_key)

    # 5. 캐시 크기 초과 시 가장 오래된 항목 제거
    if len(_cache_order) > EMBEDDING_CACHE_SIZE:
        oldest_key = _cache_order.pop(0)
        del _embedding_cache[oldest_key]

    return embedding
```

### 5.3 컬렉션 계층 구조 (L0/L1/L2)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      컬렉션 계층 구조                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   L0 (이번 턴)     ─────► BM25 weight = 3 (가장 높음)                   │
│   │                                                                      │
│   │  "방금 사용자가 말한 '당뇨', '항암치료'"                            │
│   │                                                                      │
│   ▼                                                                      │
│   L1 (이번 세션)   ─────► BM25 weight = 2                               │
│   │                                                                      │
│   │  "이번 대화에서 누적된 정보 (ephemeral_collection)"                 │
│   │                                                                      │
│   ▼                                                                      │
│   L2 (DB 저장)     ─────► BM25 weight = 1 (가장 낮음)                   │
│                                                                          │
│       "DB에 영구 저장된 사용자의 과거 병력/상태"                        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

왜 계층 구조인가?
─────────────────
1. 현재 질문에 더 관련 있는 정보에 높은 가중치
2. 대화 컨텍스트 일관성 유지
3. 사용자의 즉각적 의도 반영
```

---

## 6. LangGraph 파이프라인

### 6.1 그래프 구성 코드

```python
# app/agents/new_pipeline.py

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite import SqliteSaver

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

    # Entry point
    graph.set_entry_point("session_orchestrator")
    graph.add_edge("session_orchestrator", "router")

    # 조건부 분기 (핵심!)
    graph.add_conditional_edges(
        "router",
        route_edge,
        {
            "info_extractor": "info_extractor",
            "persist_pipeline": "persist_pipeline",
            END: END,
        },
    )

    # 기본 흐름
    graph.add_edge("info_extractor", "user_context")
    graph.add_edge("user_context", "policy_retriever")
    graph.add_edge("policy_retriever", "answer_llm")
    graph.add_edge("persist_pipeline", END)

    # 체크포인터 (SQLite)
    checkpointer = SqliteSaver(conn)
    return graph.compile(checkpointer=checkpointer)
```

### 6.2 조건부 라우팅

```python
def route_edge(state: State):
    nxt = state.get("next") or "info_extractor"
    action = state.get("user_action")

    if nxt == "end":
        if action == "reset_drop":
            return END
        if action == "reset_save" or state.get("end_session"):
            return "persist_pipeline"
        if action == "save":
            return "persist_pipeline"
        return END

    return "info_extractor"
```

---

## 7. 핵심 모듈 분석

### 7.1 info_extractor (정보 추출)

**역할**: 사용자 발화에서 프로필/병력 정보 자동 추출

```python
# Pydantic 스키마
class ExtractedProfile(BaseModel):
    age: Optional[ProfileField]              # 나이
    region_gu: Optional[ProfileField]        # 거주 구
    median_income_ratio: Optional[ProfileField]  # 중위소득 비율
    disability_grade: Optional[ProfileField]     # 장애등급 (0/1/2)
    pregnancy_status: Optional[ProfileField]     # 임신상태
    # ...

class Triple(BaseModel):
    subject: str      # "self", "child", "spouse" 등
    predicate: str    # "disease", "treatment" 등
    object: str       # 자유 텍스트
    code: Optional[str]  # "E11", "C509" 등
    confidence: float    # 0~1

class ExtractResult(BaseModel):
    profile: ExtractedProfile
    collection: ExtractedCollection
    residual_query: Optional[str]  # 정보 제거 후 남은 질문
```

**복합 문장 분리**:
```
입력: "저는 중위소득 50%고, 임신중인데 받을 수 있는 혜택이 뭐가 있나요?"

추출 결과:
- profile.median_income_ratio = {"value": "50", "confidence": 0.95}
- profile.pregnancy_status = {"value": "임신중", "confidence": 0.9}
- residual_query = "받을 수 있는 혜택이 뭐가 있나요?"
```

### 7.2 policy_retriever (정책 검색)

**핵심 파라미터**:
```python
RAW_TOP_K = 12              # 초기 검색 문서 수
CONTEXT_TOP_K = 8           # LLM에 전달할 최대 문서 수
SIMILARITY_FLOOR = 0.3      # 유사도 임계값
BM25_WEIGHT = 0.2           # 하이브리드 검색에서 BM25 비율
EMBEDDING_CACHE_SIZE = 100  # 임베딩 캐시 크기

# 컬렉션 계층별 가중치
LAYER_WEIGHTS = {
    "L0": 3,  # 이번 턴 추출
    "L1": 2,  # 이번 세션
    "L2": 1,  # DB 저장
}
```

### 7.3 llm_answer_creator (응답 생성)

**시스템 프롬프트**:
```python
SYSTEM_PROMPT = """
당신은 의료·복지 지원 정책 안내 상담사입니다.

## 중요: 추가 필터링 금지
- 제공된 정책 문서는 이미 사용자 프로필 기반으로 필터링이 완료된 상태입니다
- 제공된 모든 정책을 사용자에게 안내해야 합니다

## 답변 형식
1. **한 줄 결론** (굵게)
2. 각 정책마다:
   - 정책명 + 지역
   - 지원 내용 (benefits 기반)
   - 지원 자격 (requirements 기반)
   - URL
3. 다음 단계 안내

## 제약
- 제공된 컨텍스트만 사용, 추측 금지
- **제공된 모든 정책을 출력** (임의로 개수를 줄이지 않음)
"""
```

**스트리밍 구현**:
```python
def run_answer_llm_stream(input_text, used, profile_ctx, ...):
    stream = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
        stream=True,
    )

    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            yield chunk.choices[0].delta.content
```

---

## 8. 데이터베이스 설계

### 8.1 주요 테이블

```sql
-- users 테이블
users:
  ├─ id (UUID, PK)
  ├─ username (VARCHAR, UNIQUE)
  ├─ password_hash (VARCHAR)
  ├─ main_profile_id (FK → profiles.id)
  └─ created_at, updated_at

-- profiles 테이블
profiles:
  ├─ id (INT, PK)
  ├─ user_id (FK → users.id)
  ├─ name (VARCHAR)
  ├─ sex (ENUM: M/F)
  ├─ birth_date (DATE)
  ├─ residency_sgg_code (VARCHAR)
  ├─ insurance_type (ENUM)
  ├─ median_income_ratio (FLOAT)
  ├─ disability_grade (INT: 0/1/2)
  └─ pregnant_or_postpartum12m (BOOLEAN)

-- documents 테이블 (정책 DB)
documents:
  ├─ id (INT, PK)
  ├─ title (VARCHAR)
  ├─ requirements (TEXT)
  ├─ benefits (TEXT)
  ├─ region (VARCHAR)
  └─ url (VARCHAR)

-- embeddings 테이블 (PGVector)
embeddings:
  ├─ id (INT, PK)
  ├─ doc_id (FK → documents.id)
  ├─ field (VARCHAR)  -- 'title'
  └─ embedding (vector(1536))
```

### 8.2 필드 매핑 (프론트엔드 ↔ DB)

| 프론트엔드 | DB |
|-----------|-----|
| name | name |
| gender | sex (M/F) |
| birthDate | birth_date |
| location | residency_sgg_code |
| healthInsurance | insurance_type |
| incomeLevel | median_income_ratio |
| disabilityLevel | disability_grade |
| pregnancyStatus | pregnant_or_postpartum12m |

---

## 9. 성능 최적화

### 9.1 임베딩 캐싱

```python
_embedding_cache: Dict[str, List[float]] = {}
# FIFO 방식, 최대 100개
# 캐시 히트: ~0.001s
# 캐시 미스: ~0.8s
```

### 9.2 체크포인터 전환

| 특성 | MemorySaver | SqliteSaver |
|------|-------------|-------------|
| 저장 위치 | RAM | 디스크 |
| 재시작 시 | **손실** | **유지** |
| 메모리 | **계속 증가** | 일정 |
| 동시성 | 단일 스레드 | **WAL 모드** |

```python
_checkpointer_conn = sqlite3.connect(
    CHECKPOINT_DB_PATH,
    check_same_thread=False,
)
_checkpointer_conn.execute("PRAGMA journal_mode=WAL")
_checkpointer_conn.execute("PRAGMA synchronous=NORMAL")
```

### 9.3 DB 연결 풀

```python
_connection_pool = ConnectionPool(
    conninfo=DB_URL,
    min_size=2,
    max_size=10,
    timeout=30,
    max_lifetime=300,  # 5분마다 재활용
)
```

### 9.4 스트리밍 응답

- **비스트리밍**: 전체 응답 생성 후 전송 (3~5초 대기)
- **스트리밍**: 첫 토큰까지 0.5~1초, 이후 실시간 표시

---

## 10. 디자인 패턴

| 패턴 | 적용 위치 | 이유 |
|------|----------|------|
| **Singleton** | LangGraph 앱, OpenAI 클라이언트 | 초기화 비용 절감 |
| **Repository** | user_repository.py | DB 접근 추상화 |
| **Strategy** | 지역별 크롤러 | 크롤링 전략 분리 |
| **Factory** | crawler_factory.py | 크롤러 생성 추상화 |
| **Adapter** | UserProfile.to_db_dict() | 필드명 변환 |
| **State Machine** | LangGraph | 대화 상태 관리 |

---

## 11. 면접 예상 질문 & 답변

### 일반 질문

#### Q1. "왜 LangGraph를 선택했나요?"

> **A:** "복잡한 대화 흐름을 **상태 머신 기반**으로 관리하기 위해서입니다. 각 노드(세션 관리, 정보 추출, 검색, 응답 생성)가 독립적으로 동작하면서도 상태를 공유할 수 있어, 멀티턴 대화와 세션 타임아웃 같은 기능을 체계적으로 구현할 수 있었습니다."

#### Q2. "하이브리드 검색에서 0.8:0.2 비율은 어떻게 정했나요?"

> **A:** "실험을 통해 결정했습니다. 의료 정책 특성상 의미적 유사성(벡터)이 더 중요하지만, '임산부', '장애인' 같은 키워드 매칭도 필요해서 BM25를 20% 반영했습니다. 이 비율에서 정확도가 가장 높았습니다."

#### Q3. "SqliteSaver로 전환한 이유는?"

> **A:** "MemorySaver는 서버 재시작 시 세션이 손실되고 메모리가 계속 누적됩니다. SqliteSaver는 디스크 기반이라 영구 저장되고, WAL 모드로 동시성도 우수합니다."

### AI/RAG 관련 질문

#### Q4. "RAG에서 왜 하이브리드 검색을 사용하나요?"

> **A:** "순수 벡터 검색만 사용하면 **의미적으로 유사하지만 핵심 키워드가 없는 문서**가 상위에 올라올 수 있습니다. 예를 들어 '당뇨 환자 지원'을 검색할 때, '만성질환 관리 사업'이 의미적으로 유사할 수 있지만 '당뇨'라는 키워드가 없을 수 있죠. BM25를 20% 반영하면 키워드 매칭도 고려되어 정확도가 올라갑니다."

#### Q5. "컬렉션 계층(L0/L1/L2)은 왜 만들었나요?"

> **A:** "**대화의 시간적 맥락**을 반영하기 위해서입니다.
> - L0(이번 턴): 방금 말한 정보가 가장 중요
> - L1(이번 세션): 이전 턴에서 말한 정보도 관련
> - L2(DB): 오래전 등록한 정보는 가중치 낮게
>
> BM25 re-ranking에서 L0에 3배, L1에 2배, L2에 1배 가중치를 줘서 **현재 의도에 더 부합하는 정책**을 상위에 노출합니다."

#### Q6. "info_extractor에서 residual_query는 왜 필요한가요?"

> **A:** "사용자가 **정보 입력과 질문을 한 문장에 섞어서** 말할 때가 많습니다.
>
> 예: '저는 당뇨가 있고 동작구에 사는데 혜택이 뭐가 있나요?'
> - 정보: '당뇨', '동작구' → ephemeral_profile/collection에 저장
> - 질문: '혜택이 뭐가 있나요?' → residual_query로 분리
>
> 이렇게 분리하면 정책 검색 쿼리가 더 깔끔해집니다."

#### Q7. "임베딩 모델로 text-embedding-3-small을 선택한 이유는?"

> **A:** "세 가지 이유입니다:
> 1. **비용 효율**: ada-002 대비 5배 저렴
> 2. **성능**: 1536차원으로 충분한 표현력
> 3. **속도**: 평균 0.8초 응답 시간"

#### Q8. "스트리밍 응답의 장점은?"

> **A:** "**체감 응답 시간 단축**입니다.
> - 비스트리밍: 전체 응답 생성 후 한 번에 전송 (3~5초 대기)
> - 스트리밍: 첫 토큰까지 0.5~1초, 이후 실시간 표시
>
> 사용자는 AI가 '생각하고 있다'는 느낌을 받아 UX가 개선됩니다."

#### Q9. "PGVector에서 <=> 연산자는 무엇인가요?"

> **A:** "**코사인 거리(Cosine Distance)** 연산자입니다.
> ```sql
> ORDER BY embedding <=> query_vector
> ```
> 코사인 유사도 = 1 - 코사인 거리이고, 거리가 작을수록 유사합니다."

#### Q10. "temperature=0.0과 0.3의 차이는?"

> **A:** "**결정성 vs 창의성** 트레이드오프입니다.
> - **info_extractor (temp=0.0)**: 정보 추출은 결정적이어야 함
> - **llm_answer_creator (temp=0.3)**: 자연스러운 응답 필요, 하지만 환각 방지"

#### Q11. "Synthetic Query는 언제 사용하나요?"

> **A:** "질문이 너무 일반적일 때입니다. '혜택 알려주세요' 같은 경우 그대로 임베딩하면 의미 있는 검색이 안 됩니다. 그래서 프로필+컬렉션 정보를 조합해 '동작구 거주; 당뇨병 환자; 관련 의료·복지 지원 정책' 같은 의미 있는 쿼리를 만듭니다."

---

## 12. 핵심 숫자 요약

| 항목 | 값 | 설명 |
|------|-----|------|
| 임베딩 차원 | 1536 | text-embedding-3-small |
| 캐시 크기 | 100개 | FIFO 방식 |
| RAW_TOP_K | 12 | 초기 검색 문서 수 |
| CONTEXT_TOP_K | 8 | LLM에 전달할 최대 문서 수 |
| SIMILARITY_FLOOR | 0.3 | 유사도 임계값 |
| BM25_WEIGHT | 0.2 | 하이브리드 검색에서 BM25 비율 |
| L0 가중치 | 3 | 이번 턴 키워드 |
| L1 가중치 | 2 | 이번 세션 키워드 |
| L2 가중치 | 1 | DB 저장 키워드 |
| rolling_summary 주기 | 15턴 | 대화 요약 업데이트 |
| Access Token 만료 | 24시간 | JWT |
| Refresh Token 만료 | 30일 | JWT |
| 세션 타임아웃 | 15분 | 비활성 세션 종료 |

---

## 13. 코드 핵심 파일 위치

| 기능 | 파일 |
|------|------|
| FastAPI 진입점 | `app/main.py` |
| JWT 인증 | `app/auth.py` |
| 사용자 API | `app/api/v1/user.py` |
| 채팅 API | `app/api/v1/chat.py` |
| LangGraph 빌더 | `app/agents/new_pipeline.py` |
| State 정의 | `app/langgraph/state/ephemeral_context.py` |
| 정보 추출 | `app/langgraph/nodes/info_extractor.py` |
| 컨텍스트 구성 | `app/langgraph/nodes/user_context_node.py` |
| 정책 검색 (RAG) | `app/langgraph/nodes/policy_retriever.py` |
| 응답 생성 | `app/langgraph/nodes/llm_answer_creator.py` |
| 프로필 필터링 | `app/langgraph/utils/retrieval_filters.py` |
| 필드 변환 | `app/schemas.py` |

---

*이 문서는 Poly 프로젝트 면접 준비를 위해 작성되었습니다.*
