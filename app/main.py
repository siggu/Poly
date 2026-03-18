# app/main.py
from __future__ import annotations
import logging
from contextlib import asynccontextmanager

from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.api.v1 import user, chat


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 시작/종료 시 실행되는 lifespan 이벤트
    - Startup: OpenAI 클라이언트 및 DB 연결 풀 사전 초기화
    - Shutdown: 필요한 정리 작업 수행
    """
    logger.info("=" * 60)
    logger.info("서비스 사전 초기화 중...")

    try:
        from app.langgraph.nodes.policy_retriever import _get_openai_client

        _get_openai_client()
        logger.info("OpenAI 클라이언트 초기화 완료")
    except Exception as e:
        logger.warning(f"OpenAI 클라이언트 초기화 실패: {e}")

    try:
        from app.langgraph.nodes.policy_retriever import _get_connection_pool

        _get_connection_pool()
        logger.info("DB 연결 풀 초기화 완료")
    except Exception as e:
        logger.warning(f"DB 연결 풀 초기화 실패: {e}")

    try:
        from app.api.v1.chat import get_graph_app

        get_graph_app()
        logger.info("LangGraph 워크플로우 초기화 완료")
    except Exception as e:
        logger.warning(f"LangGraph 초기화 실패: {e}")

    logger.info("=" * 60)

    yield

    logger.info("서버 종료")


app = FastAPI(
    title="HealthInformer API",
    description="Unified /api/chat endpoint to handle entire session flow.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS 설정
cors_origins = [origin.strip() for origin in settings.CORS_ORIGINS.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 사용자 및 채팅 API 라우터 추가
app.include_router(user.router, prefix="/api/v1")
app.include_router(chat.router, prefix="/api/v1")  # /api/v1/chat


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
