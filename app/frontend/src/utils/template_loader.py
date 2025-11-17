"""템플릿 로더 유틸리티 함수들 11.13 수정"""
import os
import streamlit as st
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Determine the base directory of the Streamlit app (app/stream_app)
# This file is in app/stream_app/src/utils
STREAM_APP_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TEMPLATES_DIR = os.path.join(STREAM_APP_BASE_DIR, "templates")


def get_template_path(template_name: str) -> Path:
    """템플릿 파일 경로 반환"""
    base_dir = Path(__file__).parent.parent.parent
    template_path = base_dir / "templates" / template_name
    return template_path


def load_template(template_name: str, **kwargs) -> str:
    """템플릿 파일을 로드하고 변수 치환"""
    try:
        template_path = get_template_path(template_name)
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 변수 치환
        if kwargs:
            content = content.format(**kwargs)
        
        return content
    except FileNotFoundError:
        st.warning(f"템플릿 파일을 찾을 수 없습니다: {template_name}")
        return ""


def render_template(template_name: str, **kwargs):
    """템플릿을 렌더링하여 Streamlit에 표시"""
    content = load_template(template_name, **kwargs)
    if content:
        st.markdown(content, unsafe_allow_html=True)


def load_css(css_name: str):
    """CSS 파일을 로드하여 Streamlit에 주입"""
    full_path = os.path.join(STREAM_APP_BASE_DIR, "styles", css_name)
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            css_content = f.read()

        st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        logger.error(f"CSS 파일을 찾을 수 없습니다: {full_path}")
        st.error(f"CSS 파일을 찾을 수 없습니다: {css_name}")
    except Exception as e:
        logger.error(f"CSS 파일을 로드하는 중 오류 발생: {full_path} - {e}")
        st.error(f"CSS 파일을 로드하는 중 오류 발생: {css_name} - {e}")
