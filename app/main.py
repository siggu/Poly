# app/main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import FastAPI
from app.api.v1 import user, chat

app = FastAPI(
    title="HealthInformer API",
    description="Unified /api/chat endpoint to handle entire session flow.",
)

# 사용자 및 채팅 API 라우터 추가
app.include_router(user.router, prefix="/api/v1")
app.include_router(chat.router, prefix="/api")


# ─────────────────────────────────────
# python main.py 로도 실행되게 옵션 추가 (선택)
# ─────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
