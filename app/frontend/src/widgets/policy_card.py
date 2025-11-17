""" 정책 카드 위젯 관련 함수들 """
import streamlit as st
import uuid
from typing import Dict, Any
from src.utils.template_loader import render_template, load_css


def render_policy_card(policy: Dict[str, Any]):
    """PolicyCard.tsx를 대체하는 Streamlit 위젯"""
    # CSS 로드
    load_css("components/policy_card.css")
    
    unique_key = f"policy_{policy.get('id', str(uuid.uuid4()))}"
    is_eligible = policy.get("isEligible", True)
    title = policy.get("title", "제목 없음")

    # 스타일 설정
    if is_eligible:
        card_style = "border: 2px solid #4CAF50; padding: 15px; border-radius: 8px; margin: 10px 0;"
        badge_color = "#4CAF50"
        badge_text = "신청 가능"
    else:
        card_style = "border: 2px solid #FF5722; padding: 15px; border-radius: 8px; margin: 10px 0;"
        badge_color = "#FF5722"
        badge_text = "신청 불가"

    # 카드 헤더와 설명
    render_template(
        "components/policy_card.html",
        card_style=card_style,
        title=title,
        badge_color=badge_color,
        badge_text=badge_text,
        description=policy.get('description', '설명 없음')
    )

    # 부적합 사유 표시
    if not is_eligible and policy.get("ineligibilityReason"):
        st.warning(f"⚠️ {policy['ineligibilityReason']}")

    # 상세 정보
    with st.expander(f"전체 보기 - {title}"):
        st.markdown("### 자격 요건")
        st.write(policy.get("eligibility", "자격 요건 정보 없음"))

        st.markdown("### 지원 내용")
        st.write(policy.get("benefits", "지원 내용 정보 없음"))

    # 버튼 영역
    cols = st.columns([1, 1])
    with cols[1]:
        if is_eligible and policy.get("applicationUrl"):
            if st.button("신청하기", key=f"{unique_key}_apply"):
                st.markdown(f"[신청 페이지로 이동]({policy['applicationUrl']})")


def render_error_message(error_type: str, message: str):
    """에러 메시지 표시"""
    st.error(f"**{error_type}**: {message}")
