"""
구별 특수 크롤러 모듈
"""

from .songpa_crawler import SongpaCrawler
from .yangcheon_crawler import YangcheonCrawler
from .district_menu_crawler import DistrictMenuCrawler
from .ehealth_crawler import EHealthCrawler
from .welfare_crawler import WelfareCrawler

__all__ = [
    "SongpaCrawler",
    "YangcheonCrawler",
    "DistrictMenuCrawler",
    "EHealthCrawler",
    "WelfareCrawler",
]
