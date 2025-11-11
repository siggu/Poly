# -*- coding: utf-8 -*-
# init_all_schemas.py
# 목적: 의료복지 RAG 서비스 핵심 스키마 일괄 초기화 (users / profiles / triples / (옵션) eligibility_snapshot)
#
# 사용 예:
#   python init_all_schemas.py
#   python init_all_schemas.py --drop                  # 기존 객체 삭제 후 재생성
#   python init_all_schemas.py --reset truncate        # 데이터만 전부 비우고(IDENTITY 초기화) 제약 유지
#   python init_all_schemas.py --no-fk                 # triples/profile FK 생략(부트스트랩 용)
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
parser.add_argument("--reset", choices=["none", "truncate"], default="none", help="데이터 리셋 방식 (none|truncate)")
parser.add_argument("--no-fk", action="store_true", help="FK 제약 생략 (부트스트랩용)")
parser.add_argument("--with-snapshot", action="store_true", help="eligibility_snapshot 테이블도 생성")
args = parser.parse_args()

# ----------------------------
# DROP (안전한 순서: children → parents → enums)
# ----------------------------
DROP_SQL = r"""
-- children
DROP TABLE IF EXISTS triples CASCADE;
DROP TABLE IF EXISTS eligibility_snapshot CASCADE;

-- parents
DROP TABLE IF EXISTS profiles CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- enums
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
# CREATE users
# ----------------------------
USERS_SQL = r"""
CREATE TABLE IF NOT EXISTS users (
  id              TEXT PRIMARY KEY,
  username        TEXT UNIQUE NOT NULL,
  password_hash   TEXT NOT NULL,
  main_profile_id BIGINT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- updated_at 자동 업데이트
CREATE OR REPLACE FUNCTION set_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE PROCEDURE set_users_updated_at();
"""

# ----------------------------
# CREATE profiles (기존 core_profile 대체) - 9개 항목
# ----------------------------
PROFILES_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS profiles (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL {fk_users},

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

CREATE INDEX IF NOT EXISTS idx_profiles_user_id      ON profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_profiles_residency    ON profiles(residency_sgg_code);
CREATE INDEX IF NOT EXISTS idx_profiles_insurance    ON profiles(insurance_type);
CREATE INDEX IF NOT EXISTS idx_profiles_income       ON profiles(median_income_ratio);
"""

# users.main_profile_id FK + 소유 일치성 보장
USERS_PROFILES_FK_AND_TRIGGERS = r"""
-- users.main_profile_id → profiles.id
ALTER TABLE users
  ADD CONSTRAINT fk_users_main_profile
  FOREIGN KEY (main_profile_id) REFERENCES profiles(id) ON DELETE SET NULL;

-- main_profile_id의 소유일치 검증
CREATE OR REPLACE FUNCTION ensure_main_profile_belongs_to_user()
RETURNS TRIGGER AS $$
DECLARE
  prof_user_id TEXT;
BEGIN
  IF NEW.main_profile_id IS NULL THEN
    RETURN NEW;
  END IF;
  SELECT user_id INTO prof_user_id FROM profiles WHERE id = NEW.main_profile_id;
  IF prof_user_id IS NULL OR prof_user_id <> NEW.id THEN
    RAISE EXCEPTION 'main_profile_id % does not belong to user %', NEW.main_profile_id, NEW.id;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_main_profile_check ON users;
CREATE TRIGGER trg_users_main_profile_check
BEFORE INSERT OR UPDATE OF main_profile_id ON users
FOR EACH ROW EXECUTE PROCEDURE ensure_main_profile_belongs_to_user();

-- "해당 유저의 첫 번째 프로필이 자동으로 main_profile_id가 된다"
CREATE OR REPLACE FUNCTION set_main_profile_on_first()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE users
     SET main_profile_id = NEW.id,
         updated_at = NOW()
   WHERE id = NEW.user_id
     AND main_profile_id IS NULL;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_profiles_first_sets_main ON profiles;
CREATE TRIGGER trg_profiles_first_sets_main
AFTER INSERT ON profiles
FOR EACH ROW EXECUTE PROCEDURE set_main_profile_on_first();
"""

# ----------------------------
# CREATE triples (SPO 컬렉션) - profile_id 기반
# ----------------------------
TRIPLES_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS triples (
  id BIGSERIAL PRIMARY KEY,
  profile_id BIGINT NOT NULL {fk_profiles},
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

CREATE INDEX IF NOT EXISTS idx_triples_profile_pred ON triples (profile_id, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_code         ON triples (code_system, code);
CREATE INDEX IF NOT EXISTS idx_triples_created      ON triples (created_at);
"""

# ----------------------------
# CREATE eligibility_snapshot (옵션) - profile_id 기반
# ----------------------------
SNAPSHOT_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS eligibility_snapshot (
  snapshot_id BIGSERIAL PRIMARY KEY,
  profile_id BIGINT NOT NULL {fk_profiles},
  as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
  rule_version TEXT NOT NULL,
  inputs JSONB NOT NULL,
  outputs JSONB NOT NULL,
  decision TEXT NOT NULL,                -- APPROVED/REJECTED/REVIEW 등
  explanation TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_profile ON eligibility_snapshot(profile_id, as_of_date);
"""

# ----------------------------
# TRUNCATE (자료만 리셋)
# ----------------------------
TRUNCATE_SQL = r"""
TRUNCATE TABLE
  triples,
  eligibility_snapshot,
  profiles,
  users
RESTART IDENTITY CASCADE;
"""

def exec_sql(cur, sql: str, label: str):
    print(f"→ {label} ...")
    cur.execute(sql)

def build_profiles_sql(no_fk: bool) -> str:
    fk_users = "" if no_fk else "REFERENCES users(id) ON DELETE CASCADE"
    return PROFILES_SQL_TEMPLATE.format(fk_users=fk_users)

def build_triples_sql(no_fk: bool) -> str:
    fk_profiles = "" if no_fk else "REFERENCES profiles(id) ON DELETE CASCADE"
    return TRIPLES_SQL_TEMPLATE.format(fk_profiles=fk_profiles)

def build_snapshot_sql(no_fk: bool) -> str:
    fk_profiles = "" if no_fk else "REFERENCES profiles(id) ON DELETE CASCADE"
    return SNAPSHOT_SQL_TEMPLATE.format(fk_profiles=fk_profiles)

with psycopg.connect(DB_URL, autocommit=True) as conn:
    with conn.cursor() as cur:
        if args.drop:
            exec_sql(cur, DROP_SQL, "DROP existing tables & enums")

        # enums → users → profiles → (users<->profiles FK & triggers) → triples → snapshot
        exec_sql(cur, ENUMS_SQL, "CREATE enums")
        exec_sql(cur, USERS_SQL, "CREATE users")
        exec_sql(cur, build_profiles_sql(args.no_fk), "CREATE profiles")
        if not args.no_fk:
            exec_sql(cur, USERS_PROFILES_FK_AND_TRIGGERS, "ADD users<->profiles FK & triggers (main_profile default=first)")

        exec_sql(cur, build_triples_sql(args.no_fk), "CREATE triples")

        if args.with_snapshot:
            exec_sql(cur, build_snapshot_sql(args.no_fk), "CREATE eligibility_snapshot")

        if args.reset == "truncate":
            exec_sql(cur, TRUNCATE_SQL, "TRUNCATE all data (restart identities)")

print("✅ Initialization completed.")
