"""중앙집중 설정 모듈 - 모든 환경변수를 Pydantic BaseSettings로 관리"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = ""
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = ""
    DB_USER: str = ""
    DB_PASSWORD: str = ""

    # Auth
    SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24시간
    REFRESH_TOKEN_EXPIRE_MINUTES: int = 43200  # 30일

    # OpenAI
    OPENAI_API_KEY: str = ""
    ANSWER_MODEL: str = "gpt-4o-mini"
    EMBEDDING_MODEL: str = "text-embedding-3-small"

    # CORS
    CORS_ORIGINS: str = "http://localhost:8501"

    # LangGraph
    CHECKPOINT_DB_PATH: str = "./checkpoints.db"

    # Session
    SESSION_IDLE_TIMEOUT_SEC: int = 900
    SESSION_MAX_TURNS: int = 128
    SESSION_MAX_DURATION_SEC: int = 7200

    # Router / Extractor
    ROUTER_MODEL: str = "gpt-4o-mini"

    # Policy Retriever
    POLICY_RETRIEVER_RAW_TOP_K: int = 8
    POLICY_RETRIEVER_CONTEXT_TOP_K: int = 5
    POLICY_RETRIEVER_SIM_FLOOR: float = 0.3
    POLICY_RETRIEVER_MIN_AFTER_FLOOR: int = 5
    POLICY_RETRIEVER_BM25_WEIGHT: float = 0.2

    # Persist / Cleaner
    PERSIST_ENABLE_CLEANER: bool = True
    PERSIST_CLEANER_MODE: str = "full"
    PERSIST_NO_STORE_POLICY: str = "redact"
    PERSIST_CLEANER_MAX_BYTES: int = 4096

    # Frontend
    FASTAPI_BASE_URL: str = "http://localhost:8000"

    # LangSmith
    LANGSMITH_TRACING: bool = False
    LANGSMITH_API_KEY: str = ""
    LANGSMITH_PROJECT: str = "pr-medical-chatbot"
    LANGSMITH_ENDPOINT: str = "https://api.smith.langchain.com"

    model_config = {"env_file": ".env", "case_sensitive": True, "extra": "ignore"}


settings = Settings()
