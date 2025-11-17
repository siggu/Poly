# """ 11.13 수정 fastAPI 백엔드 서버 - 챗봇용으로 변경
# 이 파일은 FastAPI 백엔드 서버의 메인 실행 파일입니다."""
# from fastapi import FastAPI, HTTPException, status
# from pydantic import BaseModel
# import sys
# import os
# import uvicorn

# # 주의: 이 파일은 ML 예측을 위한 코드가 아닌, 챗봇 백엔드 API 서버 역할을 합니다.

# # ----------------------------------------------------
# # 1. DB 모듈 Import 경로 설정
# # ----------------------------------------------------
# # app/db 폴더의 database.py를 import하기 위해 시스템 경로를 추가합니다.
# # (backend/api_server.py 에서 ..app 경로를 찾음)
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# sys.path.append(os.path.join(PROJECT_ROOT, "app"))

# try:
#     from db.database import get_db_connection

#     # from db.user_repository import UserRepository
# except ImportError as e:
#     print(f"FATAL ERROR: DB 모듈 로드 실패. 경로를 확인하세요: {e}")


# # ----------------------------------------------------
# # 2. Pydantic 모델 정의 (챗봇용으로 변경)
# # ----------------------------------------------------
# # Streamlit에서 요청받을 데이터 모델
# class ChatRequest(BaseModel):
#     user_id: str
#     question: str
#     # ML 예시 코드의 avg_temp, min_temp 등을 대신합니다.


# # 챗봇 응답 모델
# class ChatResponse(BaseModel):
#     answer: str
#     is_success: bool = True


# # ----------------------------------------------------
# # 3. FastAPI 인스턴스 초기화
# # ----------------------------------------------------
# # 기존 코드와 동일하게 인스턴스 초기화
# app = FastAPI(title="Policy Chatbot Backend API")


# # ----------------------------------------------------
# # 4. 엔드포인트: DB 연결 테스트 (신규 추가)
# # ----------------------------------------------------


# @app.get("/db-test")
# def db_test():
#     """DB 연결을 시도하고 성공 여부를 반환합니다."""
#     conn = get_db_connection()

#     if conn is None:
#         raise HTTPException(
#             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#             detail="Database connection failed. Check .env file and DB server.",
#         )

#     conn.close()
#     return {
#         "status": "Database connection successful!",
#         "message": "FastAPI is connected to PostgreSQL.",
#     }


# # ----------------------------------------------------
# # 5. 핵심 엔드포인트: 챗봇 (LangGraph 통합 지점)
# # ----------------------------------------------------


# @app.post("/chat", response_model=ChatResponse)
# def chat_handler(request: ChatRequest):
#     """
#     Streamlit의 채팅 요청을 처리하고 LangGraph가 통합될 때까지 더미 응답을 반환합니다.
#     (기존 ML 예측 엔드포인트를 챗봇 엔드포인트로 대체)
#     """
#     print(f"요청 수신: User ID: {request.user_id}, 질문: {request.question}")

#     # LangGraph 로직이 통합될 위치입니다.
#     # 현재는 DB 연결 확인을 위한 더미 응답만 반환합니다.
#     dummy_answer = f"[{request.user_id}님] 질문 '{request.question}'을 잘 받았습니다. 백엔드 연결 확인 완료! (LangGraph 작업 중)"

#     return ChatResponse(answer=dummy_answer)


# # ----------------------------------------------------
# # 6. 서버 실행 (if __name__ == "__main__": 블록)
# # ----------------------------------------------------
# if __name__ == "__main__":
#     # 기존 ML 예시 코드의 실행 블록을 유지하되, 포트를 8000으로 변경합니다.
#     # (uvicorn backend.api_server:app --reload --port 8000 명령어와 동일)
#     uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
