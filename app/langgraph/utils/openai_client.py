"""OpenAI 클라이언트 싱글턴 모듈"""
from openai import OpenAI
from app.config import settings

_client = None


def get_openai_client() -> OpenAI:
    """OpenAI 클라이언트 싱글턴 반환"""
    global _client
    if _client is None:
        if not settings.OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        _client = OpenAI(api_key=settings.OPENAI_API_KEY)
    return _client
