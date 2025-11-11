# init_all_schemas.py
# 목적: 의료복지 RAG 서비스의 핵심 스키마 일괄 초기화
#  - core_profile (프로필 9개 항목)
#  - triples (SPO 컬렉션)
#  - (옵션) eligibility_snapshot (판정 스냅샷)
#
# 사용 예:
#   python init_all_schemas.py
#   python init_all_schemas.py --drop                  # 기존 객체 삭제 후 재생성
#   python init_all_schemas.py --no-fk                 # triples의 FK 생략(부트스트랩 용)
#   python init_all_schemas.py --with-snapshot         # eligibility_snapshot 포함 생성
#
# 환경변수:
#   DATABASE_URL = postgresql://user:pass@host:5432/dbname
#
# 의존:
#   pip install psycopg python-dotenv

import os
import argparse
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (e.g. postgresql://user:pass@localhost:5432/db)")

# psycopg는 postgresql:// 스킴을 기대
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

parser = argparse.ArgumentParser(description="Initialize all DB schemas for medical-welfare RAG service")
parser.add_argument("--drop", action="store_true", help="기존 테이블/ENUM 삭제 후 재생성")
parser.add_argument("--no-fk", action="store_true", help="triples.user_id의 FK 제약 생략")
parser.add_argument("--with-snapshot", action="store_true", help="eligibility_snapshot 테이블도 생성")
args = parser.parse_args()

# ----------------------------
# DROP (안전한 순서: FK→참조→ENUM)
# ----------------------------
DROP_SQL = r"""
-- 1) children first
DROP TABLE IF EXISTS triples CASCADE;
DROP TABLE IF EXISTS eligibility_snapshot CASCADE;

-- 2) parents
DROP TABLE IF EXISTS core_profile CASCADE;

-- 3) enums
DO $$ BEGIN
  DROP TYPE IF EXISTS insurance_type CASCADE;
EXCEPTION WHEN undefined_object THEN NULL; END $$;

DO $$ BEGIN
  DROP TYPE IF EXISTS basic_benefit_type CASCADE;
EXCEPTION WHEN undefined_object THEN NULL; END $$;

DO $$ BEGIN
  DROP TYPE IF EXISTS ltci_grade CASCADE;
EXCEPTION WHEN undefined_object THEN NULL; END $$;
"""

# ----------------------------
# CREATE ENUMS
# ----------------------------
ENUMS_SQL = r"""
DO $$ BEGIN
  CREATE TYPE insurance_type AS ENUM ('EMPLOYED','LOCAL','DEPENDENT','MEDICAL_AID_1','MEDICAL_AID_2');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE basic_benefit_type AS ENUM ('NONE','LIVELIHOOD','MEDICAL','HOUSING','EDUCATION');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE ltci_grade AS ENUM ('NONE','G1','G2','G3','G4','G5','COGNITIVE');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
"""

# ----------------------------
# CREATE core_profile (9개 항목)
# ----------------------------
CORE_PROFILE_SQL = r"""
CREATE TABLE IF NOT EXISTS core_profile (
  user_id TEXT PRIMARY KEY,

  birth_date DATE,
  sex TEXT,  -- 'M' | 'F' | 'O' | 'N' (정규화는 앱 레벨)
  residency_sgg_code TEXT,
  insurance_type insurance_type,
  median_income_ratio NUMERIC(5,2),
  basic_benefit_type basic_benefit_type DEFAULT 'NONE',
  disability_grade SMALLINT,                           -- 0=미등록,1=심한,2=심하지 않음
  ltci_grade ltci_grade DEFAULT 'NONE',
  pregnant_or_postpartum12m BOOLEAN,

  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_disability_grade CHECK (disability_grade IN (0,1,2) OR disability_grade IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_profile_residency ON core_profile(residency_sgg_code);
CREATE INDEX IF NOT EXISTS idx_profile_insurance ON core_profile(insurance_type);
CREATE INDEX IF NOT EXISTS idx_profile_income    ON core_profile(median_income_ratio);
"""

# ----------------------------
# CREATE triples (SPO 컬렉션)
# ----------------------------
TRIPLES_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS triples (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL {fk_clause},
  subject TEXT NOT NULL,
  predicate TEXT NOT NULL,
  object TEXT NOT NULL,
  code_system TEXT,
  code TEXT,
  onset_date DATE,
  end_date DATE,
  negation BOOLEAN DEFAULT FALSE,
  confidence NUMERIC(3,2),
  source_id TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_triples_user_pred ON triples (user_id, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_code      ON triples (code_system, code);
CREATE INDEX IF NOT EXISTS idx_triples_created   ON triples (created_at);
"""

def build_triples_sql(no_fk: bool) -> str:
    fk_clause = "" if no_fk else "REFERENCES core_profile(user_id) ON DELETE CASCADE"
    return TRIPLES_SQL_TEMPLATE.format(fk_clause=fk_clause)

# ----------------------------
# CREATE eligibility_snapshot (옵션)
# ----------------------------
SNAPSHOT_SQL = r"""
CREATE TABLE IF NOT EXISTS eligibility_snapshot (
  snapshot_id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES core_profile(user_id) ON DELETE CASCADE,
  as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
  rule_version TEXT NOT NULL,
  inputs JSONB NOT NULL,
  outputs JSONB NOT NULL,
  decision TEXT NOT NULL,                -- APPROVED/REJECTED/REVIEW 등
  explanation TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_user ON eligibility_snapshot(user_id, as_of_date);
"""

def exec_sql(cur, sql: str, label: str):
    print(f"→ {label} ...")
    cur.execute(sql)

with psycopg.connect(DB_URL, autocommit=True) as conn:
    with conn.cursor() as cur:
        if args.drop:
            exec_sql(cur, DROP_SQL, "DROP existing tables & enums")

        exec_sql(cur, ENUMS_SQL, "CREATE enums")
        exec_sql(cur, CORE_PROFILE_SQL, "CREATE core_profile")
        exec_sql(cur, build_triples_sql(args.no_fk), "CREATE triples")

        if args.with_snapshot:
            exec_sql(cur, SNAPSHOT_SQL, "CREATE eligibility_snapshot")

print("✅ Initialization completed.")
