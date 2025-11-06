## 프론트 엔드 구조
 stream_app/          # 프론트엔드 (Streamlit)
 <br />
 │   │   ├── app.py           # 메인 앱
  <br />
 │   │   ├── pages/           # 페이지
  <br />
 │   │   ├── src/             # 소스 코드
  <br />
 │   │   │   ├── widgets/     # UI 위젯
  <br />
 │   │   │   ├── utils/       # 유틸리티
  <br />
 │   │   ├── templates/       # HTML 템플릿(각 컴포넌트와 관련된 HTML)
  <br />
 │   │   └── styles/          # CSS 스타일
 <br />
## 아키텍처

### 1. 프론트엔드 (Streamlit)
- **위치**: `app/stream_app/`
- **기술**: Streamlit (Python)
- **기능**:
  - 사용자 인터페이스
  - 로그인/회원가입
  - 챗봇 대화 인터페이스
  - 프로필 관리
  - 설정 관리


  ### 프론트엔드 개발
- Streamlit 위젯은 `app/stream_app/src/widgets/` 에서 관리
- HTML 템플릿은 `app/stream_app/templates/` 에서 관리
- CSS 스타일은 `app/stream_app/styles/` 에서 관리