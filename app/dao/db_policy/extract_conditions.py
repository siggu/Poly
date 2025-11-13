# -*- coding: utf-8 -*-
"""
extract_conditions.py
- documents.requirements(자연어) → eligibility_conditions(JSONB) 추출/업데이트
- 중위소득/연령/질병/수급·차상위/장애/임신·영유아/학생/보험자격/본인부담 12개월·3개월 등 규칙 기반 태깅
- '지역주민/구민' 단독 표현은 기본 거주조건으로 취급하지 않고 무시(자동 생성 방지)

Usage:
  python app/dao/db_policy/extract_conditions.py
  python app/dao/db_policy/extract_conditions.py --limit 500 --dry-run
  python app/dao/db_policy/extract_conditions.py --id 1234
  python app/dao/db_policy/extract_conditions.py --policy-id 5678

Env:
  DATABASE_URL 또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
"""

import os
import re
import sys
import json
import argparse
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# DSN
# ─────────────────────────────────────────────────────────────────────────────
def dsn_from_env() -> str:
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")
    if not all([name, user, pwd]):
        raise RuntimeError("DATABASE_URL 또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD) 필요")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

# ─────────────────────────────────────────────────────────────────────────────
# 스키마 보강
# ─────────────────────────────────────────────────────────────────────────────
def ensure_documents_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='documents' AND column_name='eligibility_conditions'
            ) THEN
                ALTER TABLE documents ADD COLUMN eligibility_conditions JSONB;
                CREATE INDEX IF NOT EXISTS idx_documents_eligibility ON documents USING GIN (eligibility_conditions);
            END IF;
        END$$;
        """)
    conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# 정규표현식 사전
# ─────────────────────────────────────────────────────────────────────────────
_WS = re.compile(r"\s+")
def _norm(s: str) -> str:
    return _WS.sub(" ", s).strip()

# 수치/연산자
RE_NUM = r"(?:\d{1,3}(?:[.,]\d{1,2})?)"
RE_OP = r"(?:이상|초과|이하|미만|내외|대|~|-)"
RE_PCT = r"(?:%|퍼센트|퍼센트포인트?)"

# ① 중위소득 비율 (예: 중위소득 150% 이하)
PAT_INCOME_RATIO = re.compile(
    rf"(?:기준\s*)?중위\s*소득(?:\s*기준)?\s*({RE_NUM})\s*{RE_PCT}\s*(이하|미만|이상|초과)?",
    re.IGNORECASE
)

# ② 건강보험료/소득파생 (여기선 표식만)
PAT_NHIS_PREMIUM = re.compile(r"(?:건강보험료|건보료)\s*(?:최근\s*(\d{1,2})\s*개월)?\s*(?:([가-힣]+)?(?:이상|이하|초과|미만))?", re.IGNORECASE)

# ③ 본인부담 총액 최근 12/3개월
PAT_OOP_WINDOW = re.compile(r"(?:최근|직전)\s*(\d{1,2})\s*개월\s*(?:본인\s*부담|본인부담|의료비|진료비)\s*(?:총액|합계)?", re.IGNORECASE)

# ④ 연령 (예: 만 65세 이상, 6~24개월 영유아)
PAT_AGE_SINGLE = re.compile(r"(?:만\s*)?(\d{1,3})\s*세\s*(이상|초과|이하|미만)?")
PAT_AGE_RANGE  = re.compile(r"(?:만\s*)?(\d{1,3})\s*[~\-]\s*(\d{1,3})\s*세")

# ⑤ 질병/만성질환 (대표 키워드)
DISEASE_KEYWORDS = [
    "고혈압","당뇨","당뇨병","이상지질혈증","고지혈증","비만","심부전","협심증","심근경색",
    "뇌졸중","치매","만성폐쇄성폐질환","COPD","천식","우울증","골다공증","결핵","B형간염","C형간염"
]
PAT_DISEASE = re.compile("|".join(map(re.escape, DISEASE_KEYWORDS)))

# ⑥ 수급/차상위
PAT_BENEFIT_CLASS = re.compile(r"(기초생활수급자|수급권자|차상위(?:계층|가구|본인부담경감)|차상위)", re.IGNORECASE)

# ⑦ 장애/장애등급
PAT_DISABILITY = re.compile(r"(장애인|장애등록자|청각장애|시각장애|지체장애|발달장애|자폐|정신장애)")
PAT_DISABILITY_GRADE = re.compile(r"(?:장애(?:정도)?|장애등급)\s*(?:심한|심하지\s*않은|경도|중도|중증|경증|1급|2급|3급|4급|5급|6급)")

# ⑧ 임신/영유아/산모
PAT_PREGNANCY = re.compile(r"(임산부|임신부|산모|출산\s*후\s*\d+\s*개월|산후\s*돌봄)")
PAT_INFANT = re.compile(r"(영유아|영아|유아|미취학\s*아동|아동\s*\(?\d{1,2}\)?세?)")

# ⑨ 학생/학령
PAT_STUDENT = re.compile(r"(초등학생|중학생|고등학생\s*(?:[1-3]학년)?|대학생|청소년)")

# ⑩ 보험 자격
PAT_INSURANCE_TYPE = re.compile(r"(건강보험\s*(?:직장|지역)\s*가입자|의료급여\s*(?:1종|2종)|의료급여수급자)")

# ⑪ 무시(지역주민/구민 단독)
PAT_GENERIC_RESIDENCY = re.compile(r"^(?:지역주민|[가-힣]+구\s*주민|[가-힣]+구민|[가-힣]+시민)$")

# ─────────────────────────────────────────────────────────────────────────────
# 파서
# ─────────────────────────────────────────────────────────────────────────────
def parse_income_ratio(text: str):
    out = []
    for m in PAT_INCOME_RATIO.finditer(text):
        val = m.group(1).replace(",", ".")
        try:
            value = float(val)
        except:
            continue
        op = m.group(2) or "이하"
        out.append({
            "type": "income_ratio",
            "op": {"이하":"<=","미만":"<","이상":">=","초과":">"}.get(op,"<="),
            "value": value,
            "unit": "% of median",
            "source": "regex"
        })
    return out

def parse_nhis_premium(text: str):
    hits = []
    for m in PAT_NHIS_PREMIUM.finditer(text):
        window = m.group(1)
        hits.append({
            "type": "nhis_premium_condition",
            "window_months": int(window) if window else None,
            "source": "regex"
        })
    return hits

def parse_oop_window(text: str):
    out = []
    for m in PAT_OOP_WINDOW.finditer(text):
        out.append({
            "type": "medical_oop_window",
            "window_months": int(m.group(1)),
            "source": "regex"
        })
    return out

def parse_age(text: str):
    out = []
    for m in PAT_AGE_RANGE.finditer(text):
        a,b = int(m.group(1)), int(m.group(2))
        lo, hi = min(a,b), max(a,b)
        out.append({"type":"age","op":"between","min":lo,"max":hi,"unit":"years","source":"regex"})
    for m in PAT_AGE_SINGLE.finditer(text):
        val = int(m.group(1))
        op_k = (m.group(2) or "").strip()
        op = {"이상":">=","초과":">","이하":"<=","미만":"<"}.get(op_k, "==")
        out.append({"type":"age","op":op,"value":val,"unit":"years","source":"regex"})
    return _dedupe(out)

def parse_disease(text: str):
    return [{"type":"disease","name": m.group(0),"code": None,"source":"regex"}
            for m in PAT_DISEASE.finditer(text)]

def parse_benefit_class(text: str):
    return [{"type":"benefit_class","name": m.group(1),"source":"regex"}
            for m in PAT_BENEFIT_CLASS.finditer(text)]

def parse_disability(text: str):
    out = [{"type":"disability","name": m.group(1),"source":"regex"}
           for m in PAT_DISABILITY.finditer(text)]
    for m in PAT_DISABILITY_GRADE.finditer(text):
        out.append({"type":"disability_grade","name": m.group(0),"source":"regex"})
    return _dedupe(out)

def parse_pregnancy_infant(text: str):
    out = []
    for m in PAT_PREGNANCY.finditer(text):
        out.append({"type":"pregnancy_status","name": m.group(1),"source":"regex"})
    for m in PAT_INFANT.finditer(text):
        out.append({"type":"infant_child","name": m.group(0),"source":"regex"})
    return _dedupe(out)

def parse_student(text: str):
    return _dedupe([{"type":"student","name": m.group(0),"source":"regex"}
                    for m in PAT_STUDENT.finditer(text)])

def parse_insurance(text: str):
    return [{"type":"insurance_type","name": m.group(1),"source":"regex"}
            for m in PAT_INSURANCE_TYPE.finditer(text)]

def _dedupe(items):
    seen, out = set(), []
    for it in items:
        key = json.dumps(it, ensure_ascii=False, sort_keys=True)
        if key not in seen:
            seen.add(key); out.append(it)
    return out

def extract_eligibility_conditions(requirements: str):
    """
    입력: 자연어 requirements
    출력: List[Dict] (구조화된 조건 리스트)
    - '지역주민/구민' 단독 문장은 무시
    """
    if not requirements:
        return []

    text = _norm(requirements)
    # 지역주민 단독 한 줄만 있는 경우 무시
    if PAT_GENERIC_RESIDENCY.match(text):
        return []

    parts = []
    parts += parse_income_ratio(text)
    parts += parse_nhis_premium(text)
    parts += parse_oop_window(text)
    parts += parse_age(text)
    parts += parse_disease(text)
    parts += parse_benefit_class(text)
    parts += parse_disability(text)
    parts += parse_pregnancy_infant(text)
    parts += parse_student(text)
    parts += parse_insurance(text)

    # 필요 시 더 많은 파서 추가 (가구원수, 거주기간, 소득분위 등)
    return _dedupe(parts)

# ─────────────────────────────────────────────────────────────────────────────
# DB 작업
# ─────────────────────────────────────────────────────────────────────────────
def build_argparser():
    p = argparse.ArgumentParser(description="documents.requirements → eligibility_conditions(JSONB) 추출/업데이트")
    p.add_argument("--limit", type=int, default=1000, help="최대 처리 문서 수")
    p.add_argument("--dry-run", action="store_true", help="DB 갱신 없이 콘솔에만 표시")
    p.add_argument("--id", type=int, help="특정 documents.id만 처리")
    p.add_argument("--policy-id", type=int, help="특정 policy_id만 처리")
    p.add_argument("--where", type=str, help="추가 WHERE 조건(SQL 조각)")
    return p

def select_targets(conn, limit=1000, doc_id=None, policy_id=None, where=None):
    clauses = ["requirements IS NOT NULL", "length(trim(requirements)) > 0"]
    params = []
    if doc_id:
        clauses.append("id = %s")
        params.append(doc_id)
    if policy_id:
        clauses.append("policy_id = %s")
        params.append(policy_id)
    if where:
        clauses.append(where)

    sql = f"""
        SELECT id, requirements
          FROM documents
         WHERE {' AND '.join(clauses)}
         ORDER BY id
         LIMIT %s
    """
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def update_row(conn, doc_id: int, conditions):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET eligibility_conditions = %s, updated_at = NOW() WHERE id = %s",
            (Json(conditions), doc_id)
        )

def main():
    args = build_argparser().parse_args()
    dsn = dsn_from_env()
    conn = psycopg2.connect(dsn)

    try:
        ensure_documents_schema(conn)
        rows = select_targets(
            conn, limit=args.limit, doc_id=args.id,
            policy_id=args.policy_id, where=args.where
        )
        if not rows:
            print("처리할 대상이 없습니다.")
            return

        print(f"대상 문서: {len(rows)}건")
        updated = 0

        for doc_id, req in rows:
            conds = extract_eligibility_conditions(req)
            if args.dry_run:
                print(f"\n[id={doc_id}]")
                print(f"  requirements: {req[:120]}{'...' if len(req)>120 else ''}")
                print("  -> conditions:")
                for c in conds:
                    print("     •", json.dumps(c, ensure_ascii=False))
            else:
                update_row(conn, doc_id, conds)
                updated += 1
                if updated % 200 == 0:
                    conn.commit()
                    print(f"진행: {updated}/{len(rows)} 커밋")

        if not args.dry_run:
            conn.commit()
            print(f"\n완료: {updated}건 업데이트 (eligibility_conditions)")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 롤백: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
