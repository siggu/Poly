"""
구별 메뉴 수집 Strategy 모듈
"""

from .base_strategy import collect_menu_links as base_collect_menu_links
from .ep_strategy import collect_menu_links as ep_collect_menu_links
from .gangdong_strategy import collect_menu_links as gangdong_collect_menu_links
from .jongno_strategy import collect_menu_links as jongno_collect_menu_links
from .jungnang_strategy import collect_menu_links as jungnang_collect_menu_links
from .ydp_strategy import collect_menu_links as ydp_collect_menu_links
from .yongsan_strategy import collect_menu_links as yongsan_collect_menu_links

__all__ = [
    "base_collect_menu_links",
    "ep_collect_menu_links",
    "gangdong_collect_menu_links",
    "jongno_collect_menu_links",
    "jungnang_collect_menu_links",
    "ydp_collect_menu_links",
    "yongsan_collect_menu_links",
]
