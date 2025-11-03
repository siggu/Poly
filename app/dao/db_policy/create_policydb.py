# -*- coding: utf-8 -*-
"""
pgvector로 embeddings.embedding을 VECTOR(D)로 전환하고, 안전하게 삽입하는 스크립트
- 기존 DOUBLE PRECISION[] -> VECTOR(D) 마이그레이션 지원
- execute_values 에서 %s::vector 캐스팅
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
        # 테이블 없으면 생성
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

    # 컬럼 타입 확인
    etype = None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT udt_name, data_type
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
        """, (table, col))
        r = cur.fetchone()
        if r:
            udt_name, data_type = r
            etype = (udt_name, data_type)

    # 이미 vector면 끝
    if etype and (etype[0] == f"vector" or etype[1] == "USER-DEFINED"):
        # (주의: information_schema.data_type 이 USER-DEFINED 로 나오는 경우 있음)
        # 차원 변경은 필요시 별도 마이그레이션 절차로 진행
        return

    # 배열이면 마이그레이션
    if etype and etype[1].lower() == "ARRAY".lower() or etype[1].lower() == "ARRAY":  # 일부 환경 대비
        migrate_embeddings_array_to_vector(conn, table, col, dim)
        return

    # 혹시 다른 타입이면 강제 변환
    if etype and etype[1].lower() != "USER-DEFINED":
        migrate_embeddings_array_to_vector(conn, table, col, dim)


def migrate_embeddings_array_to_vector(conn, table="embeddings", col="embedding", dim=DIM):
    """
    DOUBLE PRECISION[] -> VECTOR(dim) 변환
    배열을 문자열 "[a,b,...]" 로 변환해 ::vector 캐스팅 사용.
    """
    with conn.cursor() as cur:
        # NULL 안전, 차원 불일치 시 잘라내기/패딩이 필요하면 여기서 처리. (기본은 그대로)
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
    """
    벡터 리터럴 문자열을 안전하게 만든다: "[v1,v2,...]".
    - None/빈 벡터는 None 반환해서 INSERT 대상에서 제외하게 처리 권장.
    - 길이가 dim과 다르면 잘라내거나 패딩(0.0)한다.
    """
    if not vec:
        return None
    # 정밀도 제한(선택): 너무 긴 소수점을 줄여 전송크기↓
    if len(vec) > dim:
        vec = vec[:dim]
    elif len(vec) < dim:
        vec = list(vec) + [0.0] * (dim - len(vec))
    parts = (f"{float(x):.7f}" for x in vec)
    return "[" + ",".join(parts) + "]"


def insert_embeddings(conn, rows, emb_table="embeddings", emb_col="embedding"):
    """
    rows: iterable of (doc_id:int, field:str, embedding_vector:list[float])
    - 벡터는 문자열 리터럴로 만들어 %s::vector 로 넣는다.
    """
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


def main():
    dsn = dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        ensure_pgvector(conn)
        ensure_embeddings_vector_schema(conn, table="embeddings", col="embedding", dim=DIM)

        # 예시: 실제 데이터 삽입
        # rows = [(doc_id, 'title', embedding_list), ...]
        # affected = insert_embeddings(conn, rows)
        # print(f"inserted {affected} embeddings")

        print("✅ pgvector 스키마 보장 및 삽입 로직 준비 완료")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
