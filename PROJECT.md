# 📖 프로젝트 구조 개요

이 문서는 "의료 혜택 정보 제공 에이전트 챗봇" 프로젝트의 전체적인 구조를 설명합니다.

## 1. 최상위 아키텍처

이 프로젝트는 두 개의 독립적인 서버로 구성된 **클라이언트-서버 아키텍처**를 따릅니다.

- **백엔드 (Backend)**: `FastAPI` 프레임워크를 사용하여 구축되었습니다. 데이터베이스 관리, 사용자 인증, LLM 연동 등 핵심 비즈니스 로직을 처리하고, 프론트엔드에 `REST API`를 제공합니다.
- **프론트엔드 (Frontend)**: `Streamlit` 프레임워크를 사용하여 구축되었습니다. 사용자에게 보여지는 웹 UI를 렌더링하고, 사용자의 입력을 받아 백엔드 API와 통신하여 결과를 표시합니다.

```
┌──────────────────┐     HTTP 요청     ┌──────────────────┐
│ Frontend         │  (JSON, Stream)   │ Backend          │
│ (Streamlit)      ├───────────────────► (FastAPI)        │
│ :8501            │◄───────────────────┤ :8000            │
└──────────────────┘     응답          └──────────────────┘
                                              │
                                              │ DB 쿼리
                                              ▼
                                       ┌─────────────┐
                                       │ PostgreSQL  │
                                       │ Database    │
                                       └─────────────┘
```

## 2. 개발 대상 디렉토리 구조

### 2.1 백엔드 API 계층 (`app/api/`)

```
app/api/
└── v1/
    ├── chat.py              # 챗봇 API 엔드포인트
    │   ├── POST /stream     # LLM 응답 스트리밍
    │   └── POST /history    # 채팅 히스토리 조회
    │
    └── user.py              # 사용자 인증/프로필 API 엔드포인트
        ├── POST /register   # 회원가입
        ├── POST /login      # 로그인
        ├── GET /profile     # 프로필 조회
        ├── PUT /profile     # 프로필 수정
        └── DELETE /profile  # 프로필 삭제
```

**역할:**
- FastAPI 라우터 정의
- 요청/응답 검증 (Pydantic 스키마 사용)
- HTTP 상태 코드 관리
- 백엔드 비즈니스 로직 호출

### 2.2 백엔드 비즈니스 로직 (`app/backend/`)

```
app/backend/
├── api_server.py            # FastAPI 서버 설정 및 라우터 등록
│   ├── CORS 미들웨어 설정
│   ├── 예외 처리 미들웨어
│   └── 라우터 등록
│
├── llm_manager.py           # LLM 모델 연동 및 관리 로직
│   ├── LLM 모델 초기화
│   ├── 프롬프트 생성
│   ├── 모델 호출 및 응답 처리
│   └── 스트리밍 응답 생성
│
└── models.py                # 백엔드 내부 데이터 모델
    ├── ChatRequest
    ├── ChatResponse
    ├── UserProfile
    └── 기타 내부 모델
```

**역할:**
- 비즈니스 로직 구현
- LLM 모델과의 상호작용
- 데이터 처리 및 변환
- 응답 생성

### 2.3 데이터베이스 계층 (`app/db/`)

```
app/db/
├── config.py                # 데이터베이스 연결 설정
│   ├── DB 호스트, 포트, 사용자명, 비밀번호
│   ├── 연결 풀 설정
│   └── 타임아웃 설정
│
├── database.py              # 데이터베이스 연결 및 CRUD 함수
│   ├── initialize_db()      # DB 초기화 및 테이블 생성
│   ├── get_connection()     # DB 연결 획득
│   ├── create_user()        # 사용자 생성
│   ├── get_user()           # 사용자 조회
│   ├── update_user()        # 사용자 수정
│   └── delete_user()        # 사용자 삭제
│
├── db_core.py               # 데이터베이스 핵심 로직
│   ├── 트랜잭션 관리
│   ├── 쿼리 실행
│   └── 결과 매핑
│
├── normalizer.py            # 데이터 정규화
│   ├── 데이터 검증
│   ├── 형식 변환
│   └── 데이터 정제
│
└── user_repository.py       # 사용자 저장소 (Repository 패턴)
    ├── find_by_id()
    ├── find_by_email()
    ├── save()
    ├── update()
    └── delete()
```

**역할:**
- PostgreSQL 데이터베이스 연결 관리
- CRUD 작업 수행
- 데이터 검증 및 정규화
- Repository 패턴을 통한 데이터 접근 추상화

### 2.4 프론트엔드 (`app/frontend/`)

```
app/frontend/
├── app.py                   # 프론트엔드 메인 실행 파일
│   ├── 세션 상태 초기화
│   ├── 페이지 라우팅
│   └── 전체 레이아웃 관리
│
├── src/
│   ├── pages/
│   │   ├── chat.py          # 챗봇 페이지
│   │   │   ├── 메시지 입력 UI
│   │   │   ├── 메시지 표시
│   │   │   └── 스트리밍 응답 처리
│   │   │
│   │   ├── login.py         # 로그인/회원가입 페이지
│   │   │   ├── 로그인 폼
│   │   │   ├── 회원가입 폼
│   │   │   └── 인증 처리
│   │   │
│   │   ├── my_page.py       # 마이페이지
│   │   │   ├── 사용자 정보 표시
│   │   │   ├── 프로필 수정
│   │   │   └── 채팅 히스토리
│   │   │
│   │   └── settings.py      # 설정 페이지
│   │       ├── 사용자 설정
│   │       ├── 알림 설정
│   │       └── 로그아웃
│   │
│   ├── utils/
│   │   ├── session_manager.py    # 세션 관리
│   │   │   ├── 로그인 상태 관리
│   │   │   ├── 토큰 저장/조회
│   │   │   └── 세션 초기화
│   │   │
│   │   └── template_loader.py    # ���플릿 로더
│   │       ├── HTML 템플릿 로드
│   │       └── 템플릿 렌더링
│   │
│   ├── widgets/
│   │   ├── auth_widgets.py       # 인증 위젯
│   │   │   ├── 로그인 폼 위젯
│   │   │   └── 회원가입 폼 위젯
│   │   │
│   │   ├── policy_card.py        # 정책 카드 위젯
│   │   │   ├── 정책 정보 표시
│   │   │   └── 정책 상세 보기
│   │   │
│   │   └── sidebar.py            # 사이드바 위젯
│   │       ├── 네비게이션 메뉴
│   │       ├── 사용자 정보
│   │       └── 로그아웃 버튼
│   │
│   ├── backend_service.py        # 백엔드 API 통신 클라이언트
│   │   ├── login()
│   │   ├── register()
│   │   ├── stream_chat()
│   │   ├── get_profile()
│   │   ├── update_profile()
│   │   └── 기타 API 호출
│   │
│   └── state_manger.py           # Streamlit 세션 상태 관리
│       ├── 초기 상태 설정
│       ├── 상태 업데이트
│       └── 상태 초기화
│
├── styles/
│   ├── components/
│   │   ├── chat_messages.css     # 채팅 메시지 스타일
│   │   ├── chat_ui.css           # 채팅 UI 스타일
│   │   ├── landing_page.css      # 랜딩 페이지 스타일
│   │   ├── policy_card.css       # 정책 카드 스타일
│   │   └── sidebar.css           # 사이드바 스타일
│   │
│   ├── custom.css                # 커스텀 스타일
│   └── my_page.css               # 마이페이지 스타일
│
├── templates/
│   ├── components/
│   │   ├── chat_header.html      # 채팅 헤더 템플릿
│   │   ├── chat_message_assistant.html  # 어시스턴트 메시지 템플릿
│   │   ├── chat_message_user.html       # 사용자 메시지 템플릿
│   │   ├── chat_title.html       # 채팅 제목 템플릿
│   │   ├── chatbot_card.html     # 챗봇 카드 템플릿
│   │   ├── disclaimer.html       # 면책 조항 템플릿
│   │   ├── policy_card.html      # 정책 카드 템플릿
│   │   ├── sidebar_logo.html     # 사이드바 로고 템플릿
│   │   └── suggested_questions_header.html  # 제안 질문 헤더 템플릿
│   │
│   └── landing_page.html         # 랜딩 페이지 템플릿
│
├── data/                         # 프론트엔드 데이터 디렉토리
├── .streamlit/                   # Streamlit 설정 디렉토리
└── pages/                        # Streamlit ���티페이지 디렉토리 (선택사항)
```

**역할:**
- 사용자 인터페이스 렌더링
- 사용자 입력 처리
- 백엔드 API 호출
- 응답 표시 및 상태 관리

## 3. 주요 파일 상세 설명

### 3.1 API 계층 (`app/api/v1/`)

#### `chat.py`
- **목적**: 챗봇 관련 API 엔드포인트 정의
- **주요 기능**:
  - `POST /stream`: 사용자 질문에 대한 LLM 응답을 스트리밍으로 제공
  - `POST /history`: 사용자의 채팅 히스토리 조회
- **의존성**: `llm_manager`, `user_repository`

#### `user.py`
- **목적**: 사용자 인증 및 프로필 관리 API 엔드포인트 정의
- **주요 기능**:
  - `POST /register`: 새 사용자 회원가입
  - `POST /login`: 사용자 로그인 (JWT 토큰 발급)
  - `GET /profile`: 현재 사용자 프로필 조회
  - `PATCH /profile/{profile_id}`: 프로필 정보 수정
  - `GET /profiles`: 모든 프로필 조회
  - `POST /profile`: 새 프로필 추가
  - `DELETE /profile/{profile_id}`: 프로필 삭제
  - `PUT /profile/main/{profile_id}`: 메인 프로필 변경
  - `DELETE /delete`: 사용자 계정 삭제
- **의존성**: `user_repository`, `auth` (JWT 처리)
- **필드 매핑**:
  - 프론트엔드 `username` → DB `profiles.name` (사용자 이름)
  - 프론트엔드 `user_id` → DB `users.username` (아이디)
  - 프론트엔드 `name` → DB `profiles.name` (사용자 이름)
  - 프론트엔드 `password` → DB `users.password_hash` (해시된 비밀번호)

### 3.2 데이터베이스 계층 상세 (`app/db/`)

#### `db_core.py`
- **목적**: 데이터베이스 핵심 연결 기능
- **주요 기능**:
  - PostgreSQL 연결 생성
  - UUID 어댑터 등록 (psycopg2)
  - 연결 풀 관리
- **사용처**: 모든 DB 작업에서 호출

#### `normalizer.py`
- **목적**: 데이터 정규화 및 변환
- **주요 함수**:
  - `_normalize_birth_date()`: 생년월일 정규화
  - `_normalize_sex()`: 성별 정규화 (남성/여성 → M/F)
  - `_normalize_insurance_type()`: 건강보험 타입 정규화
  - `_normalize_benefit_type()`: 기초생활보장 급여 정규화
  - `_normalize_disability_grade()`: 장애 등급 정규화
  - `_normalize_ltci_grade()`: 장기요양 등급 정규화
  - `_normalize_pregnant_status()`: 임신 상태 정규화
  - `_normalize_income_ratio()`: 소득 수준 정규화

#### `user_repository.py`
- **목적**: Repository 패턴을 구현한 사용자 저장소
- **주요 메서드**:
  - `create_user_and_profile()`: 사용자 및 프로필 생성 (트랜잭션)
  - `get_user_password_hash()`: 비밀번호 해시 조회
  - `get_user_and_profile_by_id()`: UUID로 사용자 및 프로필 조회
  - `get_user_by_username()`: username으로 사용자 조회
  - `update_user_password()`: 비밀번호 업데이트
  - `update_user_main_profile_id()`: 메인 프로필 변경
  - `check_user_exists()`: 사용자 존재 여부 확인
  - `delete_user_account()`: 사용자 계정 삭제
  - `add_profile()`: 새 프로필 추가
  - `update_profile()`: 프로필 정보 업데이트
  - `delete_profile_by_id()`: 프로필 삭제
  - `get_all_profiles_by_user_id()`: 사용자의 모든 프로필 조회
- **특징**:
  - 헬퍼 함수 `_transform_db_to_api()`: DB 데이터를 API 응답 형식으로 변환
  - 모든 작업에서 normalizer 함수 사용
  - 트랜잭션 기반 데이터 일관성 보장

### 3.3 백엔드 비즈니스 로직 (`app/backend/`)

#### `api_server.py`
- **목적**: FastAPI 애플리케이션 설정 및 라우터 등록
- **주요 기능**:
  - FastAPI 앱 인스턴스 생성
  - CORS 미들웨어 설정
  - 예외 처리 미들웨어
  - API 라우터 등록
- **사용처**: `app/main.py`에서 임포트되어 ��용

#### `llm_manager.py`
- **목적**: LLM 모델 연동 및 관리
- **주요 기능**:
  - LLM 모델 초기화 및 로드
  - 사용자 질문에 대한 프롬프트 생성
  - LLM 모델 호출
  - 응답 스트리밍 처리
- **사용처**: `app/api/v1/chat.py`에서 호출

#### `models.py`
- **목적**: 백엔드 내부 데이터 모델 정의
- **주요 모델**:
  - `ChatRequest`: 채팅 요청 데이터
  - `ChatResponse`: 채팅 응답 데이터
  - `UserProfile`: 사용자 프로필 데이터
  - 기타 내부 데이터 모델

### 3.4 프론트엔드 (`app/frontend/`)

#### `app.py`
- **목적**: Streamlit 애플리케이션의 메인 진입점
- **주요 기능**:
  - 세션 상태 초기화
  - 로그인 상태에 따른 페이지 라우팅
  - 전체 레이아웃 관리 (사이드바, 메인 콘텐츠)

#### `src/pages/login.py`
- **목적**: 로그인 및 회원가입 페이지
- **주요 기능**:
  - 로그인 폼 렌더링
  - 회원가입 폼 렌더링
  - 사용자 입력 검증
  - `backend_service`를 통한 API 호출

#### `src/pages/chat.py`
- **목적**: 메인 챗봇 인터페이스
- **주요 기능**:
  - 메시지 입력 UI
  - 메시지 히스토리 표시
  - `backend_service.stream_chat()`을 통한 스트리밍 응답 처리
  - 실시간 응답 표시

#### `src/backend_service.py`
- **목적**: 프론트엔드와 백엔드 간의 통신을 담당하는 클라이언트
- **주요 메서드**:
  - `login(email, password)`: 로그인 요청
  - `register(email, password, name)`: 회원가입 요청
  - `stream_chat(message)`: 채팅 스트리밍 요청
  - `get_profile()`: 프로필 조회
  - `update_profile(profile_data)`: 프로필 수정
- **중요성**: UI 컴포넌트가 직접 API를 호출하지 않고 이 서비스를 통해 통신

#### `src/state_manger.py`
- **목적**: Streamlit 세션 상태 관리
- **주요 기능**:
  - 초기 상태 설정
  - 로그인 상태 관리
  - 사용자 정보 저장
  - 채팅 히스토리 관리

## 4. 데이터 흐름

### 4.1 사용자 인증 흐름
```
사용자 입력 (로그인/회원가입)
    ↓
frontend/src/pages/login.py
    ↓
backend_service.login() / backend_service.register()
    ↓
HTTP POST 요청
    ↓
FastAPI: /api/v1/user/login, /api/v1/user/register
    ↓
app/api/v1/user.py (라우터)
    ↓
app/db/user_repository.py (사용자 검증/생성)
    ↓
PostgreSQL Database
    ↓
JWT 토큰 생성 및 반환
    ↓
Streamlit 세션에 저장
    ↓
프론트엔드 상태 업데이트
```

### 4.2 챗봇 상호작용 흐름
```
사용자 질문 입력
    ↓
frontend/src/pages/chat.py
    ↓
backend_service.stream_chat(message)
    ↓
HTTP POST 요청 (스트리밍)
    ↓
FastAPI: /api/v1/chat/stream
    ↓
app/api/v1/chat.py (라우터)
    ↓
app/backend/llm_manager.py (LLM 처리)
    ├─ 프롬프트 생성
    ├─ LLM 모델 호출
    └─ 응답 생성
    ↓
스트리밍 응답 반환
    ↓
프론트엔드에서 실시간 표시
```

### 4.3 프로필 관리 흐름
```
사용자 프로필 수정 요청
    ↓
frontend/src/pages/my_page.py
    ↓
backend_service.update_profile(profile_data)
    ↓
HTTP PUT 요청
    ↓
FastAPI: /api/v1/user/profile
    ↓
app/api/v1/user.py (라우터)
    ↓
app/db/user_repository.py (데이터 업데이트)
    ↓
PostgreSQL Database
    ↓
업데이트 결과 반환
    ↓
프론트엔드 상태 업데이트
```

## 5. 기술 스택

- **백엔드**: FastAPI, Python 3.8+, PostgreSQL, psycopg2
- **프론트엔드**: Streamlit, Python 3.8+, requests
- **인증**: JWT (JSON Web Tokens), passlib
- **LLM**: LLM 모델 연동 (llm_manager)
- **데이터베이스**: PostgreSQL

## 6. 실행 방법

### 6.1 백엔드 실행
```bash
cd app
python main.py
# 또는
uvicorn app.main:app --reload --port 8000
```

### 6.2 프론트엔드 실행
```bash
cd app/frontend
streamlit run app.py
```

## 7. 개발 체크리스트

### 백엔드 개발 (`app/api`, `app/backend`, `app/db`)
- [ ] `app/db/config.py`: 데이터베이스 연결 설정
- [ ] `app/db/database.py`: 기본 CRUD 함수 구현
- [ ] `app/db/user_repository.py`: Repository 패턴 구현
- [ ] `app/backend/models.py`: 데이터 모델 정의
- [ ] `app/backend/llm_manager.py`: LLM 연동 로직 구현
- [ ] `app/backend/api_server.py`: FastAPI 서버 설정
- [ ] `app/api/v1/user.py`: 사용자 인증 API 구현
- [ ] `app/api/v1/chat.py`: 챗봇 API 구현
- [ ] `app/main.py`: 메인 진입점 설정

### 프론트엔드 개발 (`app/frontend`)
- [ ] `app/frontend/src/state_manger.py`: 세션 상태 관리
- [ ] `app/frontend/src/backend_service.py`: 백엔드 API 클라이언트
- [ ] `app/frontend/src/utils/session_manager.py`: 세션 관리 유틸리티
- [ ] `app/frontend/src/pages/login.py`: 로그인/회원가입 페이지
- [ ] `app/frontend/src/pages/chat.py`: 챗봇 페이지
- [ ] `app/frontend/src/pages/my_page.py`: 마이페이지
- [ ] `app/frontend/src/pages/settings.py`: 설정 페이지
- [ ] `app/frontend/src/widgets/auth_widgets.py`: 인증 위젯
- [ ] `app/frontend/src/widgets/sidebar.py`: 사이드바 위젯
- [ ] `app/frontend/src/widgets/policy_card.py`: 정책 카드 위젯
- [ ] `app/frontend/app.py`: 메인 진입점

## 8. 주요 개발 포인트

### 백엔드
1. **데이터베이스 연결**: PostgreSQL 연결 풀 설정 및 관리
2. **사용자 인증**: JWT 토큰 기반 인증 구현
3. **LLM 통합**: LLM 모델 호출 및 스트리밍 응답 처리
4. **에러 처리**: 적절한 HTTP 상태 코드 및 에러 메시지 반환

### 프론트엔드
1. **세션 관리**: Streamlit 세션 상태를 통한 로그인 상태 유지
2. **API 통신**: `backend_service`를 통한 일관된 API 호출
3. **UI/UX**: 직관적인 사용자 인터페이스 구현
4. **실시간 업데이트**: 스트리밍 응답 처리 및 실시간 표시

## 9. 회원가입 및 로그인 흐름 상세

### 9.1 회원가입 흐름
```
프론트엔드 (login.py)
├─ 사용자 입력: email (아이디), username (이름), password
│
└─ backend_service.register_user()
   │
   └─ POST /api/v1/user/register
      │
      └─ app/api/v1/user.py (register_user)
         ├─ 아이디 중복 확인
         ├─ 비밀번호 해시 (bcrypt)
         │
         └─ db_ops.create_user_and_profile()
            │
            ├─ 1. users 테이블 삽입
            │   ├─ id: UUID 생성
            │   ├─ username: email 값 저장
            │   ├─ password_hash: 해시된 비밀번호
            │   └─ id_uuid: UUID 복사본
            │
            ├─ 2. profiles 테이블 삽입
            │   ├─ user_id: users.id 참조
            │   ├─ name: username 값 저장
            │   ├─ 기타 필드: normalizer로 정규화
            │   └─ RETURNING id (새 profile_id)
            │
            ├─ 3. collections 테이블 삽입
            │   └─ 초기 컬렉션 데이터
            │
            └─ 4. users.main_profile_id 업데이트
               └─ 새로 생성된 profile_id 저장
```

### 9.2 로그인 흐름
```
프론트엔드 (login.py)
├─ 사용자 입력: email (아이디), password
│
└─ backend_service.login_user()
   │
   └─ POST /api/v1/user/login
      │
      └─ app/api/v1/user.py (login_user)
         ├─ db_ops.get_user_password_hash(email)
         │  └─ users 테이블에서 username=email로 조회
         │
         ├─ 비밀번호 검증 (bcrypt.verify)
         │
         └─ JWT 토큰 생성
            ├─ payload: {"sub": email}
            ├─ 만료 시간: 30분
            └─ 토큰 반환
```

### 9.3 자동 로그인 흐름 (회원가입 후)
```
회원가입 성공
│
└─ handle_signup_submit()
   │
   ├─ 1. 회원가입 성공 확인
   │
   └─ 2. 자동 로그인 시도
      └─ backend_service.login_user(email, password)
         │
         └─ JWT 토큰 획득
            │
            └─ 세션에 토큰 저장
               │
               └─ 프론트엔드 상태 업데이트
                  └─ is_logged_in = True
```

## 10. 환경 설정

프로젝트 실행 전 다음 파일들을 확인하세요:
- `requirements.txt`: Python 의존성 설치
- `app/db/config.py`: 데이터베이스 연결 설정
- `.env` (필요시): 환경 변수 설정 (DB 비밀번호, API 키 등)
