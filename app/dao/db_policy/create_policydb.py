# -*- coding: utf-8 -*-
"""
create_policydb.py
- 정책 DB 초기화/검증/마이그레이션 스크립트
- pgvector 확장 설치 및 embeddings.embedding을 VECTOR(D)로 보장
- documents 스키마 생성/보강(eval_target, eval_content 등) + updated_at 트리거
- execute_values 에서 %s::vector 캐스팅으로 안전 대량 삽입 지원

Usage:
  python create_policydb.py [--dim 1024] [--migrate-only] [--drop-all] [--reindex] [--check]

Env:
  DATABASE_URL  또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
"""

import os
import sys
import argparse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────────
DEFAULT_DIM = 1024  # dragonkue/BGE-m3-ko기준. 모델 바뀌면 옵션 --dim 사용.

# ──────────────────────────────────────────────────────────────────
# DSN
# ──────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────
def ensure_pgvector(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    conn.commit()

def _table_exists(cur, name: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
    """, (name,))
    return cur.fetchone() is not None

def _column_info(cur, table: str, column: str):
    cur.execute("""
        SELECT udt_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
    """, (table, column))
    return cur.fetchone()  # (udt_name, data_type, char_len) or None

# ──────────────────────────────────────────────────────────────────
# documents 스키마 (생성 + 보강)
# ──────────────────────────────────────────────────────────────────
def ensure_documents_schema(conn):
    with conn.cursor() as cur:
        # 테이블이 없으면 생성
        if not _table_exists(cur, "documents"):
            cur.execute("""
                CREATE TABLE documents (
                    id                       BIGSERIAL PRIMARY KEY,
                    title                    TEXT,
                    requirements             TEXT,
                    benefits                 TEXT,
                    raw_text                 TEXT,
                    url                      TEXT,
                    policy_id                BIGINT,
                    region                   TEXT,
                    sitename                 TEXT,
                    weight                   NUMERIC,
                    eval_target              INTEGER,
                    eval_content             INTEGER,
                    llm_reinforced           BOOLEAN DEFAULT FALSE,
                    llm_reinforced_sources   JSONB,
                    created_at               TIMESTAMPTZ DEFAULT NOW(),
                    updated_at               TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # 인덱스들
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_policy   ON documents(policy_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_region   ON documents(region)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url      ON documents(url)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_sitename ON documents(sitename)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_weight   ON documents(weight)")
        else:
            # 누락된 컬럼 보강
            def _ensure_column(name: str, ddl: str):
                cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='documents' AND column_name='{name}'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN {ddl};
                    END IF;
                END$$;
                """)
            _ensure_column("title",                  "title TEXT")
            _ensure_column("requirements",           "requirements TEXT")
            _ensure_column("benefits",               "benefits TEXT")
            _ensure_column("raw_text",               "raw_text TEXT")
            _ensure_column("url",                    "url TEXT")
            _ensure_column("policy_id",              "policy_id BIGINT")
            _ensure_column("region",                 "region TEXT")
            _ensure_column("sitename",               "sitename TEXT")
            _ensure_column("weight",                 "weight NUMERIC")
            _ensure_column("eval_target",            "eval_target INTEGER")
            _ensure_column("eval_content",           "eval_content INTEGER")
            _ensure_column("llm_reinforced",         "llm_reinforced BOOLEAN DEFAULT FALSE")
            _ensure_column("llm_reinforced_sources", "llm_reinforced_sources JSONB")
            _ensure_column("created_at",             "created_at TIMESTAMPTZ DEFAULT NOW()")
            _ensure_column("updated_at",             "updated_at TIMESTAMPTZ DEFAULT NOW()")

            # 인덱스 보강
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_policy   ON documents(policy_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_region   ON documents(region)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url      ON documents(url)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_sitename ON documents(sitename)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_weight   ON documents(weight)")

        # ─────────────────────────────────────────────
        # updated_at 트리거 생성 (달러 태그 분리)
        # ─────────────────────────────────────────────
        cur.execute("""
        CREATE OR REPLACE FUNCTION set_documents_updated_at()
        RETURNS TRIGGER AS $fn$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $fn$ LANGUAGE plpgsql;
        """)

        cur.execute("""
        DO $do$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'trg_documents_updated_at'
            ) THEN
                CREATE TRIGGER trg_documents_updated_at
                BEFORE UPDATE ON documents
                FOR EACH ROW
                EXECUTE PROCEDURE set_documents_updated_at();
            END IF;
        END
        $do$;
        """)
    conn.commit()

# ──────────────────────────────────────────────────────────────────
# embeddings 스키마 (생성 + VECTOR(dim) 보장 + 마이그레이션)
# ──────────────────────────────────────────────────────────────────
def ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=DEFAULT_DIM):
    with conn.cursor() as cur:
        # 테이블 없으면 생성 (VECTOR(dim))
        if not _table_exists(cur, table):
            cur.execute(f"""
                CREATE TABLE {table} (
                    id         BIGSERIAL PRIMARY KEY,
                    doc_id     BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    field      TEXT NOT NULL CHECK (field IN ('title','requirements','benefits')),
                    {col}      VECTOR(%s) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (doc_id, field)
                );
            """, (dim,))
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_doc_field ON {table} (doc_id, field)")
            conn.commit()
            return

        # 이미 존재하면 타입 확인 후 필요 시 마이그레이션
        udt_name, data_type, _ = _column_info(cur, table, col) or (None, None, None)
        if udt_name == "vector" or (data_type and data_type.upper() == "USER-DEFINED"):
            # 벡터인데 차원이 다른 경우만 강제 변경
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} TYPE vector(%s);", (dim,))
        else:
            # 배열 등 → vector(dim) 변환
            cur.execute(f"""
                ALTER TABLE {table}
                ALTER COLUMN {col} TYPE vector(%s)
                USING (
                    CASE
                      WHEN {col} IS NULL THEN NULL
                      ELSE ( '[' || array_to_string({col}, ',') || ']' )::vector
                    END
                );
            """, (dim,))
        # 인덱스 보강
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_doc_field ON {table} (doc_id, field)")
    conn.commit()

# ──────────────────────────────────────────────────────────────────
# 벡터 리터럴/삽입 도우미
# ──────────────────────────────────────────────────────────────────
def build_vector_literal(vec, dim=DEFAULT_DIM) -> str | None:
    if not vec:
        return None
    if len(vec) > dim:
        vec = vec[:dim]
    elif len(vec) < dim:
        vec = list(vec) + [0.0] * (dim - len(vec))
    parts = (f"{float(x):.7f}" for x in vec)
    return "[" + ",".join(parts) + "]"

def insert_embeddings(conn, rows, emb_table="embeddings", emb_col="embedding"):
    """
    rows: Iterable[(doc_id:int, field:str, vector:list[float])]
    """
    to_insert = []
    for doc_id, field, vec in rows:
        lit = build_vector_literal(vec)
        if lit is None:
            continue
        to_insert.append((doc_id, field, lit))
    if not to_insert:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {emb_table} (doc_id, field, {emb_col}) VALUES %s",
            to_insert,
            template="(%s, %s, %s::vector)",
        )
    conn.commit()
    return len(to_insert)

# ──────────────────────────────────────────────────────────────────
# 인덱스 재생성(원하면)
# ──────────────────────────────────────────────────────────────────
def recreate_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_region   ON documents (region)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_sitename ON documents (sitename)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_policy   ON documents (policy_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url      ON documents (url)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_weight   ON documents (weight)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_doc_field ON embeddings (doc_id, field)")
    conn.commit()

# ──────────────────────────────────────────────────────────────────
# DROP ALL (주의!)
# ──────────────────────────────────────────────────────────────────
def drop_all(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS embeddings CASCADE;")
        cur.execute("DROP TABLE IF EXISTS documents CASCADE;")
    conn.commit()

# ──────────────────────────────────────────────────────────────────
# CHECK
# ──────────────────────────────────────────────────────────────────
def check_schema(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='documents'")
        has_docs = cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='embeddings'")
        has_embs = cur.fetchone()[0] == 1
        print(f"documents 존재: {has_docs}, embeddings 존재: {has_embs}")
        if has_embs:
            cur.execute("""
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_name='embeddings' AND column_name='embedding'
            """)
            r = cur.fetchone()
            print(f"embeddings.embedding 타입: {r[0] if r else 'N/A'}")

# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Policy DB creator / migrator (pgvector)")
    ap.add_argument("--dim", type=int, default=DEFAULT_DIM, help="pgvector 차원 (기본 1024)")
    ap.add_argument("--migrate-only", action="store_true", help="테이블 생성은 건너뛰고 타입 보정/마이그레이션만 수행")
    ap.add_argument("--drop-all", action="store_true", help="⚠️ documents/embeddings 전부 삭제 후 재생성")
    ap.add_argument("--reindex", action="store_true", help="인덱스 재생성")
    ap.add_argument("--check", action="store_true", help="최종 스키마 상태 출력")
    args = ap.parse_args()

    dsn = dsn_from_env()
    conn = psycopg2.connect(dsn)

    try:
        ensure_pgvector(conn)

        if args.drop_all:
            print("⚠️ DROP ALL: documents/embeddings 삭제")
            drop_all(conn)

        if args.migrate_only:
            # 존재하는 전제 하에 타입만 보정
            ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=args.dim)
        else:
            ensure_documents_schema(conn)
            ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=args.dim)

        if args.reindex:
            recreate_indexes(conn)

        print(f"✅ 스키마 보장 완료 (VECTOR({args.dim}))")

        if args.check:
            check_schema(conn)

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
