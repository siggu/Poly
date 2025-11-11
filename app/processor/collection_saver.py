# collection_saver.py
# 목적: "Collection Saver" 노드
# - 사용자 입력 텍스트를 LLM으로 분석해 SPO 트리플 목록으로 변환
# - triples 테이블에 멱등 업서트(중복 병합) 수행
# - LangGraph 상태에 저장 결과 기록
#
# 의존:
#   pip install openai psycopg pydantic python-dotenv
#
# 환경:
#   - OPENAI_API_KEY
#   - DATABASE_URL = postgresql://user:pass@host:5432/dbname
#
# 스키마(이미 생성됨):
#   triples(id PK, user_id, subject, predicate, object, code_system, code,
#           onset_date, end_date, negation, confidence, source_id, created_at)

from __future__ import annotations

import json
import os
import re
from datetime import date
from typing import Any, Dict, List, Optional, TypedDict, Literal

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, field_validator
import psycopg

# ─────────────────────────────────────────────────────────────────────────────
# 0) 환경 설정
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (e.g. postgresql://user:pass@localhost:5432/db)")
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

OPENAI_MODEL = os.getenv("COLLECTION_EXTRACT_MODEL", "gpt-4o-mini")
OPENAI_JSON_HINT = {"type": "json_object"}  # response_format 강제

client = OpenAI()


# ─────────────────────────────────────────────────────────────────────────────
# 1) 통제 술어(최소 핵심 세트)
# ─────────────────────────────────────────────────────────────────────────────
CONTROLLED_PREDICATES = {
    "HAS_CONDITION",
    "HAS_CHRONIC_DISEASE",
    "HAS_DISABILITY",
    "UNDER_TREATMENT",
    "HAS_SANJEONGTEUKRYE",
    "PREGNANCY_STATUS",
    "HAS_INFERTILITY",
    "FINANCIAL_SHOCK",
    "HAS_DOCUMENT",
    "ELIGIBILITY_HINT",
    "TEMPORALITY",
    "DENIES",
}

ALLOWED_CODE_SYSTEMS = {"KCD10", "SNOMED", "HIRA", "ATC", "ICD10", "NONE"}


# ─────────────────────────────────────────────────────────────────────────────
# 2) LLM 프롬프트
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""당신은 의료복지 RAG 시스템의 'SPO 추출기'이다.
입력 문장에서 통제 술어 집합에 맞추어 삼중(SPO) 리스트를 JSON으로만 출력하라.

[통제 술어(엄격)]
{", ".join(sorted(CONTROLLED_PREDICATES))}

[출력 스키마]
필수 키만 포함하고, 명시되지 않은 값은 null로 채운다. confidence는 0~1.
날짜는 가능하면 YYYY-MM-DD, 없으면 YYYY-MM, 더 없으면 null.

{{
  "triples": [
    {{
      "subject": "user",
      "predicate": "<위 술어 중 하나>",
      "object": "<라벨 또는 서술>",
      "code_system": "KCD10|SNOMED|HIRA|ATC|ICD10|NONE|null",
      "code": "<표준코드 또는 null>",
      "onset_date": "YYYY-MM-DD|YYYY-MM|null",
      "end_date": "YYYY-MM-DD|YYYY-MM|null",
      "negation": true|false|null,
      "confidence": 0.0~1.0,
      "source_id": "<문서/메시지 id 또는 null>"
    }}
  ]
}}

[규칙]
- predicate는 반드시 통제 술어 셋 중 하나.
- 질병/진단은 HAS_CONDITION, 치료 중이면 UNDER_TREATMENT, 산정특례는 HAS_SANJEONGTEUKRYE.
- 부인/해당없음은 negation=true 또는 predicate=DENIES.
- 날짜가 "2025년 6월"처럼 월만 있으면 "YYYY-MM"으로.
- 자의적 추정 금지. 불명확하면 null.
- 출력은 JSON 하나만.
"""

USER_PROMPT_TMPL = """입력:
{input_text}

위 기준에 따라 triples JSON만 출력하라.
"""


# ─────────────────────────────────────────────────────────────────────────────
# 3) 유틸: 날짜 정규화
# ─────────────────────────────────────────────────────────────────────────────
def normalize_date_str(s: Optional[str]) -> Optional[str]:
    """허용 포맷: YYYY-MM-DD, YYYY-MM. 그 외는 None."""
    if s is None:
        return None
    s = s.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}-\d{2}$", s):
        return s
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4) Pydantic 모델(검증)
# ─────────────────────────────────────────────────────────────────────────────
class TripleItem(BaseModel):
    subject: str = Field(..., description="보통 'user'")
    predicate: str
    object: str
    code_system: Optional[str] = None
    code: Optional[str] = None
    onset_date: Optional[str] = None
    end_date: Optional[str] = None
    negation: Optional[bool] = False
    confidence: Optional[float] = 0.7
    source_id: Optional[str] = None

    @field_validator("predicate")
    @classmethod
    def _v_pred(cls, v: str) -> str:
        v2 = v.strip().upper()
        if v2 not in CONTROLLED_PREDICATES:
            raise ValueError(f"predicate not allowed: {v}")
        return v2

    @field_validator("code_system")
    @classmethod
    def _v_code_sys(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        m = v.strip().upper()
        return m if m in ALLOWED_CODE_SYSTEMS else None

    @field_validator("onset_date", "end_date")
    @classmethod
    def _v_dates(cls, v: Optional[str]) -> Optional[str]:
        return normalize_date_str(v)

    @field_validator("confidence")
    @classmethod
    def _v_conf(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return 0.7
        try:
            f = float(v)
        except Exception:
            return 0.7
        return max(0.0, min(1.0, f))

class TriplePayload(BaseModel):
    triples: List[TripleItem]


# ─────────────────────────────────────────────────────────────────────────────
# 5) LLM 호출
# ─────────────────────────────────────────────────────────────────────────────
def extract_triples(input_text: str, source_id: Optional[str] = None) -> List[TripleItem]:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT_TMPL.format(input_text=input_text)}
    ]
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        response_format=OPENAI_JSON_HINT,
        temperature=0
    )
    raw = (resp.choices[0].message.content or "").strip()
    data = json.loads(raw)
    payload = TriplePayload(**data)

    # source_id 보강(입력값이 있으면 비어있는 항목에 채워넣기)
    if source_id:
        for t in payload.triples:
            if not t.source_id:
                t.source_id = source_id
    return payload.triples


# ─────────────────────────────────────────────────────────────────────────────
# 6) 멱등 업서트(간단한 중복 병합)
#    - 동일 Key( user_id, predicate, (code or object), onset_date, end_date, negation )
#      존재 시: confidence = GREATEST, 비어있는 code_system/code는 보강
# ─────────────────────────────────────────────────────────────────────────────
FIND_SQL = """
SELECT id, code_system, code, confidence
FROM triples
WHERE user_id = %(user_id)s
  AND predicate = %(predicate)s
  AND COALESCE(code,'') = COALESCE(%(code)s,'')
  AND object = %(object)s
  AND COALESCE(onset_date::text,'') = COALESCE(%(onset_date)s,'')
  AND COALESCE(end_date::text,'') = COALESCE(%(end_date)s,'')
  AND COALESCE(negation,false) = COALESCE(%(negation)s,false)
LIMIT 1;
"""

INSERT_SQL = """
INSERT INTO triples (
  user_id, subject, predicate, object, code_system, code,
  onset_date, end_date, negation, confidence, source_id, created_at
) VALUES (
  %(user_id)s, %(subject)s, %(predicate)s, %(object)s, %(code_system)s, %(code)s,
  %(onset_date)s::date, %(end_date)s::date, %(negation)s, %(confidence)s, %(source_id)s, NOW()
)
RETURNING id;
"""

UPDATE_SQL = """
UPDATE triples
SET
  code_system = COALESCE(%(code_system)s, code_system),
  code = COALESCE(%(code)s, code),
  confidence = GREATEST(COALESCE(%(confidence)s, 0), COALESCE(confidence, 0)),
  source_id = COALESCE(%(source_id)s, source_id)
WHERE id = %(id)s;
"""

def _to_exact_date(d: Optional[str]) -> Optional[str]:
    """YYYY-MM → YYYY-MM-01 로 변환하여 DATE 캐스팅 가능하게."""
    if d is None:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
        return d
    if re.match(r"^\d{4}-\d{2}$", d):
        return d + "-01"
    return None

def upsert_triples(user_id: str, triples: List[TripleItem]) -> Dict[str, Any]:
    if not triples:
        return {"inserted": 0, "updated": 0}

    inserted = 0
    updated = 0

    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            for t in triples:
                # 조회 키 구성
                key_params = {
                    "user_id": user_id,
                    "predicate": t.predicate,
                    "code": t.code,
                    "object": t.object,
                    "onset_date": _to_exact_date(t.onset_date),
                    "end_date": _to_exact_date(t.end_date),
                    "negation": bool(t.negation),
                }
                cur.execute(FIND_SQL, key_params)
                row = cur.fetchone()

                params_common = {
                    "user_id": user_id,
                    "subject": t.subject,
                    "predicate": t.predicate,
                    "object": t.object,
                    "code_system": t.code_system,
                    "code": t.code,
                    "onset_date": _to_exact_date(t.onset_date),
                    "end_date": _to_exact_date(t.end_date),
                    "negation": bool(t.negation),
                    "confidence": float(t.confidence or 0.7),
                    "source_id": t.source_id,
                }

                if row is None:
                    cur.execute(INSERT_SQL, params_common)
                    _ = cur.fetchone()  # id
                    inserted += 1
                else:
                    cur.execute(UPDATE_SQL, {**params_common, "id": row[0]})
                    updated += 1

    return {"inserted": inserted, "updated": updated}


# ─────────────────────────────────────────────────────────────────────────────
# 7) LangGraph 노드 인터페이스
# ─────────────────────────────────────────────────────────────────────────────
class State(TypedDict, total=False):
    user_id: str
    input_text: str
    router: Dict[str, Any]
    triples_delta: List[Dict[str, Any]]
    persist_result: Dict[str, Any]
    source_id: str  # 선택: 메시지/문서 id

def collection_saver_node(state: State) -> State:
    """
    사전 조건:
      - router.target in {'COLLECTION','BOTH'} 일 때 호출
    동작:
      - LLM으로 SPO 배열 추출 → 검증/정규화 → DB upsert → state 갱신
    """
    user_id = state.get("user_id") or ""
    text = (state.get("input_text") or "").strip()
    source_id = state.get("source_id")
    if not user_id or not text:
        state["triples_delta"] = [{"error": "missing user_id or input_text"}]
        return state

    try:
        triples = extract_triples(text, source_id=source_id)
    except Exception as e:
        state["triples_delta"] = [{"error": f"extract_failed: {type(e).__name__}", "detail": str(e)}]
        return state

    try:
        result = upsert_triples(user_id, triples)
    except Exception as e:
        state["triples_delta"] = [{"error": f"upsert_failed: {type(e).__name__}", "detail": str(e)}]
        return state

    # 상태 기록
    state["triples_delta"] = [t.model_dump() for t in triples]
    pr = state.get("persist_result") or {}
    pr.update({"triples_inserted": result["inserted"], "triples_updated": result["updated"]})
    state["persist_result"] = pr
    return state


# ─────────────────────────────────────────────────────────────────────────────
# 8) 단독 실행 테스트
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    demo_inputs = [
        "6월에 유방암 C50.9 진단받고 항암 치료 중입니다. 진단서 있어요.",
        "작년 11월부터 혈액투석 받고 있습니다.",
        "해당사항 없어요(암 아니고 치료도 안 해요).",
    ]
    uid = os.getenv("TEST_USER_ID", "u_demo_1")

    for i, t in enumerate(demo_inputs, 1):
        s: State = {"user_id": uid, "input_text": t, "source_id": f"msg:{i}"}
        out = collection_saver_node(s)
        print(json.dumps(out.get("triples_delta", []), ensure_ascii=False, indent=2))
        print(json.dumps(out.get("persist_result", {}), ensure_ascii=False, indent=2))
        print("-" * 60)
