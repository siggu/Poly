# -*- coding: utf-8 -*-
"""
pgvector로 embeddings.embedding을 VECTOR(D)로 전환하고, 안전하게 삽입하는 스크립트
- 기존 DOUBLE PRECISION[] -> VECTOR(D) 마이그레이션 지원
- execute_values 에서 %s::vector 캐스팅
- + documents 스키마 보강(eval_scores/eval_overall 등) 추가
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

DIM = 1536  # text-embedding-3-small 기준. 모델 바꾸면 여기도 맞춰주세요.

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

def ensure_pgvector(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    conn.commit()

def ensure_documents_schema(conn):
    sql = """
    ALTER TABLE documents
        ADD COLUMN IF NOT EXISTS title TEXT,
        ADD COLUMN IF NOT EXISTS requirements TEXT,
        ADD COLUMN IF NOT EXISTS benefits TEXT,
        ADD COLUMN IF NOT EXISTS raw_text TEXT,
        ADD COLUMN IF NOT EXISTS url TEXT,
        ADD COLUMN IF NOT EXISTS policy_id BIGINT,
        ADD COLUMN IF NOT EXISTS region TEXT,
        ADD COLUMN IF NOT EXISTS sitename TEXT,
        ADD COLUMN IF NOT EXISTS weight INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS eval_scores JSONB,
        ADD COLUMN IF NOT EXISTS eval_overall INTEGER,
        ADD COLUMN IF NOT EXISTS llm_reinforced BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS llm_reinforced_sources JSONB,
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def get_col_type(conn, table: str, column: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
        """, (table, column))
        row = cur.fetchone()
        return row[0] if row else None

def ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=DIM):
    """
    - embeddings 테이블이 없으면 VECTOR(dim)로 생성
    - 있으면 컬럼 타입 확인 후:
      * double precision[] -> vector(dim)로 마이그레이션
      * 이미 vector면 패스
    """
    with conn.cursor() as cur:
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'embeddings'
            ) THEN
                CREATE TABLE embeddings (
                    id         BIGSERIAL PRIMARY KEY,
                    doc_id     BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    field      TEXT NOT NULL CHECK (field IN ('title','requirements','benefits')),
                    embedding  VECTOR(%s) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (doc_id, field)
                );
                CREATE INDEX IF NOT EXISTS idx_embeddings_doc_field ON embeddings (doc_id, field);
            END IF;
        END$$;
        """, (dim,))
        conn.commit()

    # 타입 점검 및 필요시 마이그레이션
    with conn.cursor() as cur:
        cur.execute("""
            SELECT udt_name, data_type
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
        """, (table, col))
        r = cur.fetchone()
    if r:
        udt_name, data_type = r
        # 이미 vector면 끝
        if udt_name == "vector" or data_type == "USER-DEFINED":
            return
        # 배열이면 변환
        if data_type and data_type.lower() == "array":
            migrate_embeddings_array_to_vector(conn, table, col, dim)
            return
    # 기타 타입도 강제 변환
    migrate_embeddings_array_to_vector(conn, table, col, dim)

def migrate_embeddings_array_to_vector(conn, table="embeddings", col="embedding", dim=DIM):
    with conn.cursor() as cur:
        cur.execute(f"""
            ALTER TABLE {table}
            ALTER COLUMN {col} TYPE vector({dim})
            USING (
                CASE
                  WHEN {col} IS NULL THEN NULL
                  ELSE ( '[' || array_to_string({col}, ',') || ']' )::vector
                END
            );
        """)
    conn.commit()

def build_vector_literal(vec, dim=DIM) -> str:
    if not vec:
        return None
    if len(vec) > dim:
        vec = vec[:dim]
    elif len(vec) < dim:
        vec = list(vec) + [0.0] * (dim - len(vec))
    parts = (f"{float(x):.7f}" for x in vec)
    return "[" + ",".join(parts) + "]"

def insert_embeddings(conn, rows, emb_table="embeddings", emb_col="embedding"):
    to_insert = []
    for doc_id, field, vec in rows:
        lit = build_vector_literal(vec, DIM)
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
            template="(%s, %s, %s::vector)"
        )
    conn.commit()
    return len(to_insert)
def ensure_documents_indexes(conn):
    sql = """
    CREATE INDEX IF NOT EXISTS idx_documents_region        ON documents (region);
    CREATE INDEX IF NOT EXISTS idx_documents_sitename      ON documents (sitename);
    CREATE INDEX IF NOT EXISTS idx_documents_policy_id     ON documents (policy_id);
    CREATE INDEX IF NOT EXISTS idx_documents_eval_overall  ON documents (eval_overall);
    CREATE INDEX IF NOT EXISTS idx_documents_eval_scores_gin ON documents USING GIN (eval_scores);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    dsn = dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        ensure_pgvector(conn)
        ensure_documents_schema(conn)            # ← NEW: documents 컬럼 보장
        ensure_documents_indexes(conn)   # ← 추가
        ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=DIM)
        print("✅ documents/embeddings 스키마 보장 완료")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
