"""
크롤러 구현체 모듈
"""

from .district_crawler import DistrictCrawler
from .specific_crawler.ehealth_crawler import EHealthCrawler
from .specific_crawler.welfare_crawler import WelfareCrawler
from .specific_crawler.district_menu_crawler import DistrictMenuCrawler
from .specific_crawler.songpa_crawler import SongpaCrawler
from .specific_crawler.yangcheon_crawler import YangcheonCrawler
from .specific_crawler.mapo_crawler import MapoCrawler

__all__ = [
    "DistrictCrawler",
    "EHealthCrawler",
    "WelfareCrawler",
    "DistrictMenuCrawler",
    "SongpaCrawler",
    "YangcheonCrawler",
    "MapoCrawler",
]
