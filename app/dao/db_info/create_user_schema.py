# -*- coding: utf-8 -*-
"""
create_user_schema.py
- 목적: 사용자/프로필 스키마 + 대화 스키마(conversations/messages/embeddings)까지 일괄 초기화
- 기존 테이블: users, profiles, triples, (옵션)eligibility_snapshot
- 신규 테이블: conversations(1:1 by profile), messages(1:N), conversation_embeddings(pgvector)
- 주의: Policy DB(documents/embeddings)와 collections는 건드리지 않음(조회 전용/별도 관리 가정)

사용 예:
  python create_user_schema.py
  python create_user_schema.py --drop                  # 기존 객체 삭제 후 재생성
  python create_user_schema.py --reset truncate        # 데이터만 비우고(IDENTITY 초기화) 제약 유지
  python create_user_schema.py --no-fk                 # FK 생략(부트스트랩)
  python create_user_schema.py --with-snapshot         # eligibility_snapshot 포함 생성
  python create_user_schema.py --init-conversations    # 기존 profiles마다 conversations 1:1 초기 생성

환경변수:
  DATABASE_URL = postgresql://user:pass@host:5432/dbname
  CONV_EMB_DIM (선택) = pgvector 차원 (기본 1024)
"""

import os
import argparse
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (e.g. postgresql://user:pass@localhost:5432/db)")
if DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)

DIM = int(os.getenv("CONV_EMB_DIM", "1024"))

parser = argparse.ArgumentParser(description="Initialize users/profiles + conversations/messages/embeddings schemas")
parser.add_argument("--drop", action="store_true", help="기존 테이블/ENUM 삭제 후 재생성")
parser.add_argument("--reset", choices=["none", "truncate"], default="none", help="데이터 리셋 방식 (none|truncate)")
parser.add_argument("--no-fk", action="store_true", help="FK 제약 생략 (부트스트랩용)")
parser.add_argument("--with-snapshot", action="store_true", help="eligibility_snapshot 테이블도 생성")
parser.add_argument("--init-conversations", action="store_true", help="기존 profiles마다 conversations 1:1 초기 생성")
args = parser.parse_args()

# ----------------------------
# DROP (children → parents → enums)
#   ※ collections / documents / embeddings 는 드롭/생성 대상 아님
# ----------------------------
DROP_SQL = r"""
-- 대화 관련 children
DROP TABLE IF EXISTS conversation_embeddings CASCADE;
DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS conversations CASCADE;

-- 기존 children
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
# ENUMS
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
# users (UUID PK)
# ----------------------------
USERS_SQL = r"""
CREATE TABLE IF NOT EXISTS users (
  id              UUID PRIMARY KEY,
  username        TEXT UNIQUE NOT NULL,
  password_hash   TEXT NOT NULL,
  main_profile_id BIGINT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
# profiles (BIGSERIAL PK, FK→users.id UUID)
# ----------------------------
PROFILES_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS profiles (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL {fk_users},

  name TEXT,
  birth_date DATE,
  sex TEXT,
  residency_sgg_code TEXT,
  insurance_type insurance_type,
  median_income_ratio NUMERIC(5,2),
  basic_benefit_type basic_benefit_type DEFAULT 'NONE',
  disability_grade SMALLINT,
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

USERS_PROFILES_FK_AND_TRIGGERS = r"""
ALTER TABLE users
  DROP CONSTRAINT IF EXISTS fk_users_main_profile;

ALTER TABLE users
  ADD CONSTRAINT fk_users_main_profile
  FOREIGN KEY (main_profile_id) REFERENCES profiles(id) ON DELETE SET NULL;

CREATE OR REPLACE FUNCTION ensure_main_profile_belongs_to_user()
RETURNS TRIGGER AS $$
DECLARE
  prof_user_id UUID;
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
# triples (SPO by profile_id)
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
# eligibility_snapshot (옵션)
# ----------------------------
SNAPSHOT_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS eligibility_snapshot (
  snapshot_id BIGSERIAL PRIMARY KEY,
  profile_id BIGINT NOT NULL {fk_profiles},
  as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
  rule_version TEXT NOT NULL,
  inputs JSONB NOT NULL,
  outputs JSONB NOT NULL,
  decision TEXT NOT NULL,
  explanation TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_profile ON eligibility_snapshot(profile_id, as_of_date);
"""

# ----------------------------
# conversations/messages/conversation_embeddings (신규)
#   - profiles.id (BIGINT) ↔ conversations.profile_id (BIGINT)
#   - conversations.id (UUID) ↔ messages.conversation_id / conv_embeddings.conversation_id (UUID)
# ----------------------------
CONVERSATIONS_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS conversations (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  profile_id   BIGINT NOT NULL {fk_profiles} UNIQUE,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at     TIMESTAMPTZ,
  summary      JSONB,
  model_stats  JSONB,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_conversations_profile_id ON conversations(profile_id);

CREATE OR REPLACE FUNCTION set_conversations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_conversations_updated_at ON conversations;
CREATE TRIGGER trg_conversations_updated_at
BEFORE UPDATE ON conversations
FOR EACH ROW EXECUTE PROCEDURE set_conversations_updated_at();
"""

MESSAGES_SQL = r"""
CREATE TABLE IF NOT EXISTS messages (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id  UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  turn_index       INT  NOT NULL,
  role             TEXT NOT NULL CHECK (role IN ('user','assistant','tool')),
  content          TEXT NOT NULL,
  tool_name        TEXT,
  token_usage      JSONB,
  meta             JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (conversation_id, turn_index, role)
);
CREATE INDEX IF NOT EXISTS idx_messages_conv_created ON messages(conversation_id, created_at);
"""

CONV_EMB_SQL_TEMPLATE = r"""
CREATE TABLE IF NOT EXISTS conversation_embeddings (
  conversation_id  UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  chunk_id         TEXT NOT NULL,
  embedding        VECTOR({dim}) NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (conversation_id, chunk_id)
);
"""

# ----------------------------
# TRUNCATE (자료만 리셋)
# ----------------------------
TRUNCATE_SQL = r"""
TRUNCATE TABLE
  conversation_embeddings,
  messages,
  conversations,
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

def build_conversations_sql(no_fk: bool) -> str:
    fk_profiles = "" if no_fk else "REFERENCES profiles(id) ON DELETE CASCADE"
    return CONVERSATIONS_SQL_TEMPLATE.format(fk_profiles=fk_profiles)

def build_conv_emb_sql(dim: int) -> str:
    return CONV_EMB_SQL_TEMPLATE.format(dim=dim)

def init_conversations_for_existing_profiles(cur):
    # profiles마다 conversations 1:1 생성 (없을 때만)
    print("→ INIT conversations for existing profiles ...")
    cur.execute("""
        INSERT INTO conversations (profile_id)
        SELECT p.id
          FROM profiles p
          LEFT JOIN conversations c ON c.profile_id = p.id
         WHERE c.profile_id IS NULL
    """)

with psycopg.connect(DB_URL, autocommit=True) as conn:
    with conn.cursor() as cur:
        if args.drop:
            exec_sql(cur, DROP_SQL, "DROP existing tables & enums")

        exec_sql(cur, ENUMS_SQL, "CREATE enums")
        exec_sql(cur, USERS_SQL, "CREATE users")
        exec_sql(cur, build_profiles_sql(args.no_fk), "CREATE profiles")
        if not args.no_fk:
            exec_sql(cur, USERS_PROFILES_FK_AND_TRIGGERS, "ADD users<->profiles FK & triggers")

        # 기존 컬렉션/문서/임베딩은 건드리지 않음(조회/별도 파이프라인)
        exec_sql(cur, build_triples_sql(args.no_fk), "CREATE triples")
        if args.with_snapshot:
            exec_sql(cur, build_snapshot_sql(args.no_fk), "CREATE eligibility_snapshot")

        # 신규: 대화 스키마
        exec_sql(cur, build_conversations_sql(args.no_fk), "CREATE conversations")
        exec_sql(cur, MESSAGES_SQL, "CREATE messages")
        exec_sql(cur, build_conv_emb_sql(DIM), "CREATE conversation_embeddings")

        if args.reset == "truncate":
            exec_sql(cur, TRUNCATE_SQL, "TRUNCATE all data (restart identities)")

        if args.init_conversations:
            init_conversations_for_existing_profiles(cur)

print("✅ Initialization completed.")
