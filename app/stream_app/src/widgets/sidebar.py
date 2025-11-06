import streamlit as st
import uuid
import time
from src.utils.template_loader import load_template, render_template, load_css


# --- 1. 상태 초기화 (필요한 경우 메인 파일에서 실행) ---
if "search_query" not in st.session_state:
    st.session_state.search_query = ""
if "settings_modal_open" not in st.session_state:
    st.session_state.settings_modal_open = False
if "sidebar_search_input" not in st.session_state:
    st.session_state.sidebar_search_input = ""
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# --- 2. 핸들러 함수 ---
def handle_search_update():
    """검색 입력 필드 값이 변경될 때 실행되어 메인 상태를 업데이트"""
    st.session_state.search_query = st.session_state.get("sidebar_search_input", "")


def handle_settings_click():
    """설정 버튼 클릭 시 SettingsModal 상태를 열림으로 설정"""
    st.session_state.settings_modal_open = True


def handle_new_chat():
    """새 채팅 세션 초기화"""
    st.session_state.messages = [
        {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "content": "안녕하세요! 정책 추천 챗봇입니다. 나이, 거주지, 관심 분야를 알려주시면 맞춤형 정책을 추천해드립니다.",
            "timestamp": time.time(),
        }
    ]
    st.session_state["input"] = ""
    st.session_state["is_loading"] = False
    st.rerun()


def render_sidebar():
    """좌측 사이드바 렌더링"""
    # CSS 로드
    load_css("components/sidebar.css")

    with st.sidebar:
        # SIMPLECIRCLE 로고
        render_template("components/sidebar_logo.html")

        # 검색 입력 필드
        st.text_input(
            "Q 대화 내용 검색...",
            key="sidebar_search_input",
            on_change=handle_search_update,
            placeholder="Q 대화 내용 검색...",
            label_visibility="collapsed",
        )

        st.markdown("---")

        # 정책 추천 챗봇 카드
        render_template("components/chatbot_card.html")

        st.markdown("---")

        # 새 채팅 버튼
        if st.button("➕ 새 채팅", key="btn_new_chat", use_container_width=True):
            handle_new_chat()

        st.markdown("---")

        # 채팅 내역 (히스토리)
        st.markdown("#### 채팅 내역")
        if st.session_state.get("chat_history"):
            for idx, chat in enumerate(st.session_state.chat_history):
                if st.button(
                    f"💬 {chat.get('title', f'채팅 {idx+1}')}",
                    key=f"chat_history_{idx}",
                    use_container_width=True,
                ):
                    # 채팅 로드 로직 (필요시 구현)
                    st.info(f"채팅 {idx+1}을 불러옵니다.")
        else:
            st.caption("채팅 내역이 없습니다.")

        st.markdown("---")

        # 설정 버튼 (하단 고정)
        if st.button("⚙️ 설정", key="sidebar_settings", use_container_width=True):
            st.session_state["settings_modal_open"] = True
            st.rerun()


# --- 실행 (테스트용) ---
if __name__ == "__main__":
    render_sidebar()
