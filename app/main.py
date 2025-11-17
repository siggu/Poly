"""
FastAPI 백엔드 메인 애플리케이션 파일입니다.
라우터 등록 및 미들웨어 설정을 담당합니다.
11.13 수정
"""

from dotenv import load_dotenv

# .env 파일의 환경 변수를 다른 모듈이 import 되기 전에 가장 먼저 로드합니다.
load_dotenv()


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any

# 라우터 임포트
from app.api.v1 import chat, user


# FastAPI 애플리케이션 초기화
# app = FastAPI(
#     title="Custom Welfare AI Backend",
#     description="Streamlit 앱을 위한 맞춤형 복지 AI LLM 및 DB 서비스 백엔드",
#     version="1.0.0",
# )
app = FastAPI()
# CORS 설정: Streamlit(프론트엔드)에서 백엔드 API 호출을 허용하기 위해 필수
# 개발 환경에서는 모든 출처를 허용 (*), 실제 환경에서는 Streamlit 앱 URL로 제한해야 합니다.
origins = [
    "*",  # 개발 환경에서 모든 출처 허용
    "http://localhost:8501",  # Streamlit 기본 포트
    # "http://127.0.0.1:8501",  # Streamlit 기본 포트
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 라우터 등록 ---
app.include_router(chat.router, prefix="/api/v1")
app.include_router(user.router, prefix="/api/v1")


# --- 헬스 체크 엔드포인트 ---
@app.get("/health", response_model=Dict[str, Any], tags=["Health"])
async def health_check():
    """
    백엔드 서버 상태를 확인합니다.
    Streamlit의 BackendService.health_check에서 이 엔드포인트를 호출합니다.
    """
    return {"status": "ok", "message": "FastAPI server is running."}


if __name__ == "__main__":
    import uvicorn

    # 개발 서버 실행 (Streamlit이 이 서버를 호출합니다)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
