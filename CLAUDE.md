# CLAUDE.md — Poly 프로젝트 가이드

## 프로젝트 개요

Poly는 한국어 의료 복지 정책 통합정보 제공 AI 챗봇 서비스입니다.
사용자 프로필(나이, 거주지, 보험 유형, 장애 등급 등)을 기반으로 맞춤형 의료·복지 정책을 추천합니다.

## 기술 스택

- **백엔드**: FastAPI (포트 8000), Python 3.11
- **프론트엔드**: Streamlit (포트 8501)
- **AI/LLM**: LangGraph + LangChain + OpenAI GPT-4o-mini
- **DB**: PostgreSQL + PGVector (벡터 검색), SQLite (LangGraph 체크포인트)
- **검색**: 하이브리드 검색 (PGVector 유사도 + BM25 키워드)
- **인증**: JWT (access 24h + refresh 30d) + bcrypt

## 프로젝트 구조

```
app/
├── main.py                    # FastAPI 진입점
├── auth.py                    # JWT 토큰 관리
├── schemas.py                 # Pydantic 모델
├── agents/new_pipeline.py     # LangGraph 그래프 정의
├── api/v1/
│   ├── chat.py                # 채팅 엔드포인트
│   └── user.py                # 인증/프로필 엔드포인트
├── langgraph/
│   ├── nodes/                 # 파이프라인 노드 (7개)
│   │   ├── session_orchestrator.py
│   │   ├── query_router.py
│   │   ├── info_extractor.py
│   │   ├── user_context_node.py
│   │   ├── policy_retriever.py
│   │   ├── llm_answer_creator.py
│   │   └── persist_pipeline.py
│   ├── state/                 # LangGraph 상태 스키마
│   └── utils/                 # 검색 필터, 병합 유틸
├── db/
│   ├── config.py              # DB 연결 설정
│   ├── database.py            # CRUD 헬퍼
│   ├── db_core.py             # DB 커넥션 관리
│   ├── normalizer.py          # 데이터 정규화
│   └── user_repository.py     # 유저/프로필 레포지토리
├── frontend/
│   ├── app.py                 # Streamlit 진입점
│   ├── src/pages/             # 페이지 (login, chat, my_page, settings)
│   ├── src/utils/             # 세션, 템플릿 유틸
│   ├── src/widgets/           # UI 컴포넌트
│   ├── src/backend_service.py # FastAPI HTTP 클라이언트
│   ├── styles/                # CSS
│   └── templates/             # HTML 템플릿
├── crawling/                  # 정책 데이터 크롤링
├── dao/                       # 데이터 액세스 (레거시)
├── chunking/                  # 텍스트 청킹
└── embedding/                 # 임베딩 생성
```

## 로컬 실행 방법

```bash
# 백엔드
cd app
python main.py

# 프론트엔드
cd app/frontend
streamlit run app.py
```

## 필수 환경 변수

- `OPENAI_API_KEY` — OpenAI API 키
- `DATABASE_URL` — PostgreSQL 연결 문자열 (또는 DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
- `FASTAPI_BASE_URL` — 백엔드 URL (기본: http://localhost:8000)
- `CHECKPOINT_DB_PATH` — SQLite 체크포인트 경로
- `LANGSMITH_TRACING`, `LANGSMITH_API_KEY`, `LANGSMITH_PROJECT` — LangSmith 모니터링 (선택)

## 커밋 컨벤션

Conventional Commits 스타일 사용:
- `feat:` 새 기능
- `fix:` 버그 수정
- `style:` UI/스타일 변경
- `docs:` 문서
- `refactor:` 리팩토링

## 코드 컨벤션

- 함수명: `snake_case`, 클래스명: `PascalCase`
- FastAPI 의존성 주입(`Depends()`)으로 인증/DB 관리
- Pydantic 모델로 요청/응답 검증
- 한국어 주석 및 문서

## LangGraph 파이프라인 흐름

```
session_orchestrator → query_router → info_extractor → user_context_node
    → policy_retriever → llm_answer_creator → persist_pipeline
```

- `query_router`가 쿼리 유형(chat/context/end)을 판단하여 분기
- `policy_retriever`는 하이브리드 검색(PGVector + BM25) 수행
- `llm_answer_creator`는 스트리밍 응답 생성

## 참고 사항

- 테스트 프레임워크 미설정 상태
- 배포: Oracle Cloud (백엔드) + Streamlit Cloud (프론트엔드)
- GitHub Actions로 Streamlit 슬립 방지 (4시간 간격)
- 멀티프로필 지원: 한 계정에서 가족 구성원별 프로필 관리 가능
