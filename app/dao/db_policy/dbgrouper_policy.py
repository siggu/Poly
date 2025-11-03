# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

def _build_dsn_from_env() -> str:
    """
    .env에서 DSN 구성.
    우선순위: DATABASE_URL > (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    """
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
        raise ValueError(
            "DATABASE_URL 또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD) 환경변수가 필요합니다."
        )
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"


def _ensure_indexes(cur):
    index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_documents_weight ON documents(weight)",
        "CREATE INDEX IF NOT EXISTS idx_documents_policy_null ON documents((policy_id IS NULL))",
        "CREATE INDEX IF NOT EXISTS idx_embeddings_field_doc_id ON embeddings(field, doc_id)",
        # 필요시 pgvector IVF 인덱스 (환경에 맞게 조정)
        # "CREATE INDEX IF NOT EXISTS idx_embeddings_title_ivfflat ON embeddings USING ivfflat (embeddings vector_cosine_ops) WITH (lists = 100) WHERE field = 'title'",
    ]
    for sql in index_sqls:
        cur.execute(sql)


def _reset_all_policy_ids(conn):
    """
    policy_id 전체를 NULL로 초기화.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE documents SET policy_id = NULL")
    conn.commit()
# 1) 함수 보장 헬퍼 추가
def _ensure_array_cosine_fn(conn, verbose=True):
    sql = """
    CREATE OR REPLACE FUNCTION array_cosine_similarity(a DOUBLE PRECISION[], b DOUBLE PRECISION[])
    RETURNS DOUBLE PRECISION
    LANGUAGE SQL
    IMMUTABLE
    STRICT
    AS $$
    SELECT CASE
      WHEN a IS NULL OR b IS NULL THEN NULL
      WHEN array_length(a,1) IS DISTINCT FROM array_length(b,1) THEN NULL
      ELSE (
        SELECT (SUM(a[i]*b[i])::DOUBLE PRECISION)
               / NULLIF( sqrt(SUM(a[i]*a[i])) * sqrt(SUM(b[i]*b[i])) , 0)
        FROM generate_subscripts(a,1) AS i
      )
    END
    $$;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    if verbose:
        print("[INIT] array_cosine_similarity() 보장 완료")


def assign_policy_ids(
    title_field: str = "title",
    similarity_threshold: float = 0.85,
    batch_size: int = 500,
    dry_run: bool = False,
    reset_all_on_start: bool = False,
    verbose: bool = True,
):
    """
    - '새 base'와의 유사도(sim_new)가 threshold 이상이고 기존 정책 sim_old보다 클 경우 policy_id 교체.
    - 임베딩 테이블/컬럼명은 전역 _EMB_TABLE, _EMB_COL 사용(ArgumentParser에서 세팅됨).
    """


    load_dotenv()
    if os.getenv("RESET_ALL_POLICY_IDS_ON_START", "").lower() in ("1", "true", "t", "yes", "y"):
        reset_all_on_start = True

    emb_table = globals().get("_EMB_TABLE", "embeddings")
    emb_col   = globals().get("_EMB_COL", "embedding")

    dsn = _build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        _ensure_array_cosine_fn(conn, verbose=verbose)
        with conn.cursor() as cur:
            _ensure_indexes(cur)
        conn.commit()

        if reset_all_on_start:
            if verbose:
                print("[INIT] policy_id 전체 NULL 초기화 수행")
            _reset_all_policy_ids(conn)

        # 사전 점검
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT d.id)
                FROM documents d
                JOIN {emb_table} e ON e.doc_id::bigint = d.id
                WHERE e.field = %s
                """,
                (title_field,),
            )
            docs_with_title = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM documents WHERE policy_id IS NULL")
            null_policies = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM documents d_low
                JOIN documents d_high ON d_high.weight > d_low.weight
            """)
            comparable_pairs = cur.fetchone()[0]

        if verbose:
            print(f"[CHECK] 제목임베딩 존재 문서수: {docs_with_title}")
            print(f"[CHECK] policy_id NULL 문서수: {null_policies}")
            print(f"[CHECK] weight 방향 조건 충족 가능한 페어수(rough): {comparable_pairs}")
            if docs_with_title == 0:
                print(f"[HINT] 임베딩 field가 '{title_field}'가 맞는지, 테이블/컬럼명({_EMB_TABLE}/{_EMB_COL})을 확인하세요.")

        # base 후보(=하위 weight) 가져오기: 더 이상 policy_id NULL로 제한하지 않음
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT d.id, d.weight
                FROM documents d
                WHERE EXISTS (
                    SELECT 1 FROM {emb_table} e
                    WHERE e.field = %s
                      AND e.doc_id::bigint = d.id
                )
                ORDER BY d.weight ASC, d.id ASC
                """,
                (title_field,),
            )
            bases = cur.fetchall()

        if verbose:
            print(f"[RUN] base 문서 수: {len(bases)} (batch_size={batch_size}, dry_run={dry_run})")

        total_updates = 0

        for i in range(0, len(bases), batch_size):
            batch = bases[i : i + batch_size]

            for base_id, base_weight in batch:
                with conn.cursor() as cur:
                    if dry_run:
                        # 교체/할당 가능한 대상 수만 카운트
                        count_sql = f"""
                            WITH base AS (
                                SELECT d.id AS base_id, e.{emb_col} AS base_emb
                                FROM documents d
                                JOIN {emb_table} e
                                ON e.doc_id::bigint = d.id AND e.field = %s
                                WHERE d.id = %s
                            ),
                            cand AS (
                                SELECT
                                    t.id AS target_id,
                                    et.{emb_col} AS tgt_emb
                                FROM documents t
                                JOIN {emb_table} et
                                ON et.doc_id::bigint = t.id AND et.field = %s
                                WHERE t.weight > %s
                                -- dry-run에서는 굳이 FOR UPDATE 필요 없음(계산만)
                            ),
                            sims AS (
                                SELECT
                                    c.target_id,
                                    (1 - (c.tgt_emb <=> b.base_emb)) AS sim_new,
                                    CASE
                                        WHEN pol.pol_emb IS NULL THEN NULL
                                        ELSE (1 - (c.tgt_emb <=> pol.pol_emb))
                                    END AS sim_old
                                FROM cand c
                                CROSS JOIN base b
                                LEFT JOIN LATERAL (
                                    SELECT e.{emb_col} AS pol_emb
                                    FROM {emb_table} e
                                    WHERE e.doc_id::bigint = (
                                        SELECT policy_id FROM documents WHERE id = c.target_id
                                    )
                                    AND e.field = %s
                                    LIMIT 1
                                ) pol ON TRUE
                            )
                            SELECT COUNT(*)
                            FROM sims
                            WHERE sim_new >= %s
                            AND (sim_old IS NULL OR sim_new > sim_old)
                        """
                        cur.execute(
                            count_sql,
                            (
                                title_field,  # base 임베딩 field
                                base_id,
                                title_field,  # target 임베딩 field
                                base_weight,
                                title_field,  # 기존 policy 임베딩 field
                                similarity_threshold,
                            ),
                        )
                        cnt = cur.fetchone()[0]
                                                
                    else:
                        # 실제 업데이트: sim_new >= threshold AND sim_new > sim_old일 때 교체(또는 신규 할당)
                        
                        update_sql = f"""
                            WITH base AS (
                                SELECT d.id AS base_id, e.{emb_col} AS base_emb
                                FROM documents d
                                JOIN {emb_table} e
                                ON e.doc_id::bigint = d.id AND e.field = %s
                                WHERE d.id = %s
                            ),
                            -- 대상 후보를 먼저 잠금: 오직 documents t에만 락
                            cand AS (
                                SELECT
                                    t.id AS target_id,
                                    et.{emb_col} AS tgt_emb
                                FROM documents t
                                JOIN {emb_table} et
                                ON et.doc_id::bigint = t.id AND et.field = %s
                                WHERE t.weight > %s
                                FOR UPDATE OF t SKIP LOCKED
                            ),
                            -- 기존 policy 임베딩은 LATERAL로 별도 조회 (잠금 대상 아님)
                            sims AS (
                                SELECT
                                    c.target_id,
                                    (1 - (c.tgt_emb <=> b.base_emb)) AS sim_new,
                                    CASE
                                        WHEN pol.pol_emb IS NULL THEN NULL
                                        ELSE (1 - (c.tgt_emb <=> pol.pol_emb))
                                    END AS sim_old
                                FROM cand c
                                CROSS JOIN base b
                                LEFT JOIN LATERAL (
                                    SELECT e.{emb_col} AS pol_emb
                                    FROM {emb_table} e
                                    WHERE e.doc_id::bigint = (
                                        SELECT policy_id FROM documents WHERE id = c.target_id
                                    )
                                    AND e.field = %s
                                    LIMIT 1
                                ) pol ON TRUE
                            )
                            UPDATE documents t
                            SET policy_id = (SELECT base_id FROM base)
                            FROM sims
                            WHERE t.id = sims.target_id
                            AND sims.sim_new >= %s
                            AND (sims.sim_old IS NULL OR sims.sim_new > sims.sim_old)
                            RETURNING t.id
                        """
                        cur.execute(
                            update_sql,
                            (
                                title_field,  # base 임베딩 field
                                base_id,
                                title_field,  # target 임베딩 field
                                base_weight,
                                title_field,  # 기존 policy 임베딩 field
                                similarity_threshold,
                            ),
                        )

                        updated = cur.fetchall()
                        total_updates += len(updated)

            conn.commit()
            if verbose:
                print(f"[PROGRESS] {i+len(batch)}/{len(bases)} base 처리, 누적 업데이트: {total_updates}")

        if verbose:
            print(f"[DONE] total_bases={len(bases)}, total_updates={total_updates}")

        return {
            "total_bases": len(bases),
            "total_updates": total_updates,
            "similarity_threshold": similarity_threshold,
            "batch_size": batch_size,
            "dry_run": dry_run,
            "reset_all_on_start": reset_all_on_start,
        }

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # 실행 파라미터를 간단히 하드코딩하거나 argparse로 CLI 지원
    from argparse import ArgumentParser

    p = ArgumentParser()
    p.add_argument("--title-field", default="title")
    p.add_argument("--threshold", type=float, default=0.8)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--reset-all", action="store_true",
                   help="시작 시 documents.policy_id 전체 NULL 초기화")
    p.add_argument("--verbose", action="store_true", default=True)

    # (선택) 스키마 불일치 대응 옵션
    p.add_argument("--emb-table", default=os.getenv("EMB_TABLE", "embeddings"),
                   help="임베딩 테이블명(기본 embeddings). dbloader가 doc_field_embeddings를 썼다면 여기 지정")
    p.add_argument("--emb-col", default=os.getenv("EMB_COL", "embedding"),
                   help="임베딩 컬럼명(기본 embedding). dbloader가 embedding(단수)라면 여기 지정")

    args = p.parse_args()

    # 테이블/컬럼명 오버라이드가 필요하면 전역 상수로 설정
    EMB_TABLE = args.emb_table
    EMB_COL = args.emb_col

    # SQL에서 사용할 수 있게 전역 변수처럼 바인딩
    globals()["_EMB_TABLE"] = EMB_TABLE
    globals()["_EMB_COL"] = EMB_COL

    # assign_policy_ids 내부 SQL이 이 전역을 참조하도록 아래 패치 버전으로 교체(다음 섹션 참고)
    res = assign_policy_ids(
        title_field=args.title_field,
        similarity_threshold=args.threshold,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        reset_all_on_start=args.reset_all,
        verbose=args.verbose,
    )
    print(res)