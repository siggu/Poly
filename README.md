# 🚀 서비스 이름

## Poly

> 의료 복지 정책 통합정보 제공 챗봇 서비스

</br>

# 기획 배경

**🌐 기획 배경**

의료 서비스를 받는 사람이 증가하고, 관련 복지 정책에 대해 모르는 사람이 많음

</br>

**⚠️ 문제점**

1. 정책 정보 분산

   → 시/군/구 등 여러 홈페이지에 흩어져 있음

2. 능동적 정보 수집의 어려움

   → 정보 검색 시 통합 검색 페이지로 이동

3. 지원자격 수동 비교

   → 본인이 해당하는 정책인지 비교 절차 필요

</br>

**💡해결 방안**

**내 정보와 궁금한 분야만 입력하면 관련된 정보를 알려주는 서비스를 제공**

</br>

# 기획 의도

**🤔 “내가 받을 수 있는 의료 복지 정책을 알아서 알려주면 좋지 않을까?”**

</br>

# 주요 타겟층

**1️⃣ 직접 서비스 이용이 가능한 사람**

    🧑 내가 받을수 있는 의료 정책이 궁금한 일반 시민

    🤰 의료 복지 정책이 궁금한 임신부/임산부

</br>

**2️⃣ 직접 서비스 이용이 힘든 사람**

    🧓 인터넷 사용이 서툰 디지털 노약자 대신 가족 구성원이 멀티 프로필 정보 추가

    👶 어린 자녀를 대신하여 부모가 멀티 프로필 정보 추가

</br>

# 주요 기능

**🔒 회원가입 및 로그인 기능**

사용자의 정보를 바탕으로 최적화된 정보를 제공하기 위한 회원가입 및 로그인 기능

**👥 멀티 프로필 기능**

하나의 계정에서 여러 조건으로 검색하기 위한 멀티 프로필 기능

**🤖 의료 복지 정책 제공 에이전트 챗봇 기능**

사용자의 질문과 정보에 따라 특화된 정보를 제공하는 에이전트 챗봇 기능

</br>

# 기술 스택

## 🤖 AI/ML

<aside>

|                                                      **언어 모델**                                                      |                         |                                                       AI 프레임워크                                                       |           |
| :---------------------------------------------------------------------------------------------------------------------: | :---------------------: | :-----------------------------------------------------------------------------------------------------------------------: | :-------: |
| <img src="https://registry.npmmirror.com/@lobehub/icons-static-png/latest/files/dark/gemini-color.png" width="60px" />  | Google Gemini 2.0 Flash | <img src="https://registry.npmmirror.com/@lobehub/icons-static-png/latest/files/dark/langchain-color.png" width="60px" /> | LangChain |
| <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQTSfTVLipQtEnTIPH-Z9LrFKUdE2C6CRJ3OQ&s" width="60px" /> |   OpenAI GPT-4o-mini    | <img src="https://registry.npmmirror.com/@lobehub/icons-static-png/latest/files/dark/langgraph-color.png" width="60px" /> | LangGraph |

|                                               **임베딩 모델**                                                |                     |                               **벡터 데이터베이스**                                |          |
| :----------------------------------------------------------------------------------------------------------: | :-----------------: | :--------------------------------------------------------------------------------: | :------: |
| <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQTSfTVLipQtEnTIPH-Z9LrFKUdE2C6CRJ3OQ&s" width="60px" /> | text-embedding-3-small | <img src="https://www.svgrepo.com/show/303301/postgresql-logo.svg" width="60px" /> | PGVector |

</br>

## 🔧 Backend

| <img src="https://www.svgrepo.com/show/303301/postgresql-logo.svg" width="60px" /> | PGVector |
| :--------------------------------------------------------------------------------: | :------: |
|    <img src="https://cdn.worldvectorlogo.com/logos/fastapi.svg" width="60px" />    | FastAPI  |

</br>

## 🏗️ Infra

|                                                   **클라우드 플랫폼**                                                   |              |                                                         모니터링                                                          |           |
| :---------------------------------------------------------------------------------------------------------------------: | :----------: | :-----------------------------------------------------------------------------------------------------------------------: | :-------: |
| <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRVjLnux381cQyPNffJIxZTMfRjGgnT1_Fngw&s" width="150px" > | Oracle Cloud | <img src="https://registry.npmmirror.com/@lobehub/icons-static-png/latest/files/dark/langsmith-color.png" width="60px" /> | LangSmith |

</br>

## Frontend

| <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTGDKmSgL7UJ6sstMUQTtjI2iDN7ClN2jRZ5Q&s" width="50px" /> | Streamlit |
| :---------------------------------------------------------------------------------------------------------------------: | :-------: |

</br>

## Collaboration

| <img src="https://w7.pngwing.com/pngs/940/571/png-transparent-gitea-hd-logo.png" width="60px" /> | Gitea  |
| :----------------------------------------------------------------------------------------------: | :----: |
|              <img src="https://img.icons8.com/color/512/notion.png" width="60px" />              | Notion |

<!-- todo: 패키지 구조 -->
