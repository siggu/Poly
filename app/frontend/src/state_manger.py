"""앱 전역 상태 관리를 위한 session_state 초기화 및 관리 함수들 11.13 수정"""
import streamlit as st
import uuid
import time


def initialize_session_state():
    """
    앱 전역에서 필요한 기본 session_state 키들을 안전하게 초기화
    """
    defaults = {
        "is_logged_in": False,
        "current_profile": {},  # {'id': ..., 'name': ...}
        "show_login_modal": False,
        "messages": [
            {
                "id": str(uuid.uuid4()),
                "role": "assistant",
                "content": """ 안녕하세요! 정책 추천 챗봇입니다. 
            나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.""",
                "timestamp": time.time(),
            }
        ],
        "input": "",
        "is_loading": False,
        "search_query": "",
        "sidebar_search_input": "",
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def reset_chat_session():
    """채팅 세션을 재설정"""
    st.session_state["messages"] = [
        {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": """ 안녕하세요! 정책 추천 챗봇입니다. 
            나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.""",
            "timestamp": time.time(),
        }
    ]
    st.session_state["input"] = "" 
    st.session_state["is_loading"] = False
