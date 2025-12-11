# app/main.py
from __future__ import annotations
from dotenv import load_dotenv
import os
from contextlib import asynccontextmanager

load_dotenv()

# 디버깅 로그 (선택 사항)
# print("=" * 60)
# print("🔍 [환경변수 확인]")
# db_url = os.getenv("DATABASE_URL", "NOT SET")
# print(f"  DATABASE_URL: {db_url[:60] if db_url != 'NOT SET' else 'NOT SET'}...")
# print(
#     f"  GOOGLE_API_KEY: {'✅ 설정됨' if os.getenv('GOOGLE_API_KEY') else '❌ 미설정'}"
# )
# print("=" * 60)

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.api.v1 import user, chat


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 시작/종료 시 실행되는 lifespan 이벤트
    - Startup: 임베딩 모델을 사전 로딩하여 첫 번째 사용자 요청 대기 시간 제거
    - Shutdown: 필요한 정리 작업 수행
    """
    # Startup: 임베딩 모델 사전 로딩
    print("=" * 60)
    print("🔄 임베딩 모델 사전 로딩 중...")
    try:
        from app.langgraph.nodes.policy_retriever import _get_embed_model
        _get_embed_model()  # 모델을 메모리에 미리 로드
        print("✅ 임베딩 모델 로딩 완료")
    except Exception as e:
        print(f"⚠️  임베딩 모델 로딩 실패: {e}")
        print("   (첫 번째 요청 시 로딩됩니다)")
    print("=" * 60)

    yield

    # Shutdown
    print("🛑 서버 종료")


app = FastAPI(
    title="HealthInformer API",
    description="Unified /api/chat endpoint to handle entire session flow.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS 설정 추가 (Streamlit과 통신 위해)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 사용자 및 채팅 API 라우터 추가
app.include_router(user.router, prefix="/api/v1")
app.include_router(chat.router, prefix="/api/v1")  # /api/v1/chat

# ⭐ 즉시 경로 출력 (startup 이벤트 대신)
# print("\n" + "=" * 60)
# print("📍 등록된 API 경로:")
# for route in app.routes:
#     if hasattr(route, "methods") and hasattr(route, "path"):
#         methods = ", ".join(route.methods)
#         print(f"  [{methods:12}] {route.path}")
# print("=" * 60 + "\n")


# @app.get("/health", summary="서버 상태 확인")
# def health_check():
#     return {"status": "ok", "version": "1.0.0"}


# ─────────────────────────────────────
# python main.py 로도 실행되게 옵션 추가 (선택)
# ─────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
