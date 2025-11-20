# app/langgraph/nodes/user_context_node.py
# -*- coding: utf-8 -*-
"""
user_context_node.py

역할:
  1) DB profiles / collections + ephemeral_profile / ephemeral_collection → merge
  2) merged_profile / merged_collection 기반으로 profile_summary_text 생성
  3) messages 기반 history_text(최근 대화 텍스트) 생성
  4) rolling_summary를 주기적으로 LLM으로 업데이트
  5) 컬렉션 계층(L0/L1/L2) 정보를 state에 추가
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

from app.langgraph.state.ephemeral_context import State
from app.langgraph.utils.merge_utils import merge_profile, merge_collection
from app.dao.db_user_utils import fetch_profile_from_db, fetch_collections_from_db

__all__ = ["user_context_node"]  # 이 모듈에서 내보내는 심볼 명시


# -------------------------------------------------------------------
# 코드값 → 한국어 라벨 매핑
# -------------------------------------------------------------------
INSURANCE_TYPE_LABELS = {
    "EMPLOYED": "직장가입자",
    "LOCAL": "지역가입자",
    "REGIONAL": "지역가입자",
    "DEPENDENT": "피부양자",
    "MEDICAL_AID": "의료급여 수급자",
    "NONE": None,
}

BASIC_BENEFIT_LABELS = {
    "LIVELIHOOD": "생계급여",
    "MEDICAL": "의료급여",
    "HOUSING": "주거급여",
    "EDUCATION": "교육급여",
    "NONE": None,
}

LTCI_GRADE_LABELS = {
    "LEVEL_1": "장기요양 1등급",
    "LEVEL_2": "장기요양 2등급",
    "LEVEL_3": "장기요양 3등급",
    "LEVEL_4": "장기요양 4등급",
    "LEVEL_5": "장기요양 5등급",
    "NONE": None,
    "0": None,
}

# 질환 코드/영문 → 한국어 라벨 간단 매핑
CONDITION_LABELS = {
    "diabetes": "당뇨병",
    "diabetes mellitus": "당뇨병",
    "dm": "당뇨병",
    "breast cancer": "유방암",
    "cancer": "암",
}


def _norm_code(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _map_with_labels(value: Any, labels: Dict[str, Optional[str]]) -> Optional[str]:
    code = _norm_code(value)
    if not code:
        return None
    if code in labels:
        return labels[code]
    # 매핑에 없으면 원문 그대로(이미 한글일 수도 있음)
    return str(value).strip() or None


def _map_condition_name(name: str) -> str:
    """
    영문/코드 질환명을 최대한 한국어로 바꿔주고,
    모르는 건 원문 그대로 둔다.
    """
    if not name:
        return ""
    raw = name.strip()
    code = raw.lower()
    # 간단 매핑
    for k, v in CONDITION_LABELS.items():
        if k in code:
            return v
    return raw


# -------------------------------------------------------------------
# Profile / Collection → 자연어 텍스트 요약
# -------------------------------------------------------------------
def _extract_profile_field(profile: Optional[Dict[str, Any]], key: str) -> Any:
    if not profile:
        return None
    v = profile.get(key)
    if isinstance(v, dict):
        return v.get("value")
    return v


def _profile_collection_to_text(
    profile: Optional[Dict[str, Any]],
    collection: Optional[Dict[str, Any]],
) -> str:
    """
    merged_profile / merged_collection을 LLM이 보기 쉬운 짧은 한국어 상태 요약으로 변환.
    예:
      사용자 상태: 강남구 거주; 건강보험 직장가입자; 기준중위소득 약 120% 수준;
                 기초생활보장 생계급여 수급 이력; 임신 3개월 차; 주요 질환: 당뇨병
    """
    pieces: List[str] = []

    # ---------- Profile 쪽 ----------
    if profile:
        # 1) 거주지
        region = _extract_profile_field(profile, "residency_sgg_code") or _extract_profile_field(
            profile, "region_gu"
        )
        if region:
            pieces.append(f"{region} 거주")

        # 2) 건강보험 자격
        ins_raw = _extract_profile_field(profile, "insurance_type")
        ins_label = _map_with_labels(ins_raw, INSURANCE_TYPE_LABELS)
        if ins_label:
            pieces.append(f"건강보험 {ins_label}")

        # 3) 기준중위소득 비율
        mir_raw = _extract_profile_field(profile, "median_income_ratio")
        if mir_raw is not None:
            try:
                r = float(mir_raw)
                # 0~10 사이면 비율, 10 이상이면 이미 %라고 가정
                if r <= 10:
                    pct = int(round(r * 100))
                else:
                    pct = int(round(r))
                if 0 < pct <= 300:
                    pieces.append(f"기준중위소득 약 {pct}% 수준")
            except Exception:
                pieces.append(f"소득 수준: {mir_raw}")

        # 4) 기초생활보장 급여
        basic_raw = _extract_profile_field(profile, "basic_benefit_type")
        basic_label = _map_with_labels(basic_raw, BASIC_BENEFIT_LABELS)
        if basic_label:
            pieces.append(f"기초생활보장 {basic_label} 수급 이력")

        # 5) 장애등급 (숫자 → 자연어)
        dis = _extract_profile_field(profile, "disability_grade")
        if dis is not None:
            try:
                dnum = int(float(str(dis).strip()))
            except Exception:  # noqa: BLE001
                dnum = None

            if dnum == 1:
                pieces.append("장애가 있으나 심하지 않음(경증)")
            elif dnum == 2:
                pieces.append("장애가 심함(중증)")

        # 6) 장기요양등급
        ltci_raw = _extract_profile_field(profile, "ltci_grade")
        ltci_label = _map_with_labels(ltci_raw, LTCI_GRADE_LABELS)
        if ltci_label:
            pieces.append(ltci_label)

        # 7) 임신/출산 12개월 이내 여부
        preg = _extract_profile_field(profile, "pregnant_or_postpartum12m")
        if preg:
            pieces.append("임신 중이거나 출산 후 12개월 이내")

    # ---------- Collection(triples) 쪽 ----------
    conditions: List[str] = []
    preg_text: Optional[str] = None
    has_basic_doc = False

    if collection and isinstance(collection, dict):
        triples = collection.get("triples") or []

        for t in triples:
            if not isinstance(t, dict):
                continue
            pred = (t.get("predicate") or "").strip().upper()
            obj = (t.get("object") or "").strip()
            if not obj:
                continue

            # 질환/상태 → 주요 질환으로 묶기
            if pred in ("HAS_CONDITION", "DISEASE", "HAS_DISEASE"):
                cond = _map_condition_name(obj)
                if cond:
                    conditions.append(cond)
                continue

            # 임신 상태
            if pred in ("PREGNANCY_STATUS", "PREGNANCY"):
                txt = obj.replace("달", "개월")
                preg_text = txt
                continue

            # 생계급여 서류/수급 관련
            if pred in ("HAS_DOCUMENT", "HAS_BENEFIT"):
                if "생계급여" in obj:
                    has_basic_doc = True
                continue

    if preg_text:
        pieces.append(preg_text)

    if has_basic_doc and not any("생계급여" in p for p in pieces):
        pieces.append("생계급여 수급 이력")

    if conditions:
        uniq: List[str] = []
        for c in conditions:
            if c not in uniq:
                uniq.append(c)
            if len(uniq) >= 3:
                break
        pieces.append("주요 질환: " + ", ".join(uniq))

    if not pieces:
        return ""

    return "사용자 상태: " + "; ".join(pieces)


# -------------------------------------------------------------------
# 최근 대화 히스토리 텍스트
# -------------------------------------------------------------------
def _build_history_text(state: State, max_chars: int = 600) -> str:
    """
    messages에서 최근 user/assistant 발화 몇 개를 뽑아 한글 라벨을 붙여 요약.
    """
    msgs = list(state.get("messages") or [])
    if not msgs:
        return ""

    lines: List[str] = []
    for m in msgs[-6:]:
        role = m.get("role") or "user"
        if role not in ("user", "assistant"):
            continue
        content = (m.get("content") or "").strip()
        if not content:
            continue
        prefix = "사용자" if role == "user" else "AI"
        lines.append(f"{prefix}: {content}")

    if not lines:
        return ""

    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text


# -------------------------------------------------------------------
# rolling_summary 업데이트
# -------------------------------------------------------------------
SUMMARY_PROMPT = """
다음은 지금까지의 대화 요약입니다:
<OLD_SUMMARY>
{old_summary}
</OLD_SUMMARY>

아래는 최근 턴의 메시지들입니다:
<RECENT_MESSAGES>
{recent_messages}
</RECENT_MESSAGES>

사용자가 추후 질문을 해도 컨텍스트를 잃지 않도록,
중요한 정보만 간결하고 명확하게 요약해 주세요.
"""


def _summarize(old_summary: Optional[str], messages: List[Dict[str, Any]]) -> str:
    """
    이전 summary + 최근 메시지를 기반으로 새로운 rolling_summary 생성.
    old_summary는 None일 수 있다.
    """
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    recent_text = "\n".join(
        f"{m['role']}: {m['content']}"
        for m in messages[-8:]
        if isinstance(m.get("content"), str)
    )

    prompt = SUMMARY_PROMPT.format(
        old_summary=old_summary or "",
        recent_messages=recent_text,
    )

    out = llm.invoke(prompt)
    return out.content.strip()


# -------------------------------------------------------------------
# 메인 노드 함수
# -------------------------------------------------------------------
def _normalize_collection_layer(raw: Any) -> Dict[str, Any]:
    """
    컬렉션 레이어용 보정 함수.
    - dict & "triples" 있으면 그대로
    - list 면 { "triples": list }로 감싸기
    - None 이면 빈 구조
    """
    if isinstance(raw, dict):
        if "triples" in raw:
            return {"triples": list(raw.get("triples") or [])}
        # 혹시 triples 키 없이 바로 리스트가 있는 형태라면 최대한 보정
        return {"triples": list(raw.get("triples") or [])}
    if isinstance(raw, list):
        return {"triples": list(raw)}
    return {"triples": []}


def user_context_node(state: State) -> State:
    """
    LangGraph 노드:

    입력:
      - profile_id
      - ephemeral_profile / ephemeral_collection
      - new_triples (info_extractor가 이번 턴에 추출한 triples)
      - messages / rolling_summary / turn_count

    출력/갱신:
      - merged_profile / merged_collection
      - collection_layer_L0 / L1 / L2 (계층 컬렉션)
      - profile_summary_text
      - history_text
      - rolling_summary
    """
    profile_id = state.get("profile_id")

    # 1) DB에서 profile / collections 로드
    db_profile = None
    db_collection = None
    if profile_id is not None:
        try:
            db_profile = fetch_profile_from_db(profile_id)
        except Exception as e:  # noqa: BLE001
            print(f"[user_context_node] fetch_profile_from_db error: {e}")

        try:
            db_collection = fetch_collections_from_db(profile_id)
        except Exception as e:  # noqa: BLE001
            print(f"[user_context_node] fetch_collections_from_db error: {e}")

    # 2) ephemeral과 merge (ephemeral 우선)
    eph_profile = state.get("ephemeral_profile")
    eph_collection = state.get("ephemeral_collection")

    merged_profile = merge_profile(eph_profile, db_profile)
    merged_collection = merge_collection(eph_collection, db_collection)

    state["merged_profile"] = merged_profile
    state["merged_collection"] = merged_collection

    # 2-1) 컬렉션 계층 레이어 세팅
    # L0: 이번 턴에서 info_extractor가 새로 추출한 triples
    new_triples_raw = state.get("new_triples") or []
    if not isinstance(new_triples_raw, list):
        new_triples_raw = []

    state["collection_layer_L0"] = {"triples": list(new_triples_raw)}

    # L1: 이번 세션 동안의 임시 컬렉션 (ephemeral_collection)
    state["collection_layer_L1"] = _normalize_collection_layer(eph_collection)

    # L2: DB에 저장된 기존 컬렉션
    state["collection_layer_L2"] = _normalize_collection_layer(db_collection)

    # 3) profile_summary_text 생성 (merged 기준)
    profile_summary_text = _profile_collection_to_text(merged_profile, merged_collection)
    state["profile_summary_text"] = profile_summary_text

    # 4) 최근 대화 history_text 생성
    history_text = _build_history_text(state, max_chars=600)
    state["history_text"] = history_text

    # 5) rolling_summary 업데이트 (예: 15턴마다, 메시지가 있을 때만)
    messages = state.get("messages") or []
    previous_summary = state.get("rolling_summary")
    turn_count = int(state.get("turn_count") or 1)
    should_update = (turn_count % 15 == 0)

    if should_update and messages:
        new_summary = _summarize(previous_summary, messages)
        state["rolling_summary"] = new_summary
    else:
        # 업데이트하지 않을 때도 기본값 유지
        state["rolling_summary"] = previous_summary

    return state
