"""
크롤링 베이스 클래스 모듈
"""

from .base_crawler import BaseCrawler
from .llm_crawler import LLMStructuredCrawler, HealthSupportInfo

__all__ = [
    "BaseCrawler",
    "LLMStructuredCrawler",
    "HealthSupportInfo",
]
