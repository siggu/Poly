# app/main.py
from __future__ import annotations
from dotenv import load_dotenv
import os

load_dotenv()


from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.api.v1 import user, chat


app = FastAPI(
    title="HealthInformer API",
    description="Unified /api/chat endpoint to handle entire session flow.",
    version="1.0.0",
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
