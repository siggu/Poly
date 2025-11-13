# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from psycopg2.extras import execute_values
from app.dao import utils_db

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


def _ensure_indexes(cur, emb_table: str):
    index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_documents_weight ON documents(weight)",
        "CREATE INDEX IF NOT EXISTS idx_documents_policy_null ON documents((policy_id IS NULL))",
        "CREATE INDEX IF NOT EXISTS idx_documents_policy_id ON documents(policy_id)",
        f"CREATE INDEX IF NOT EXISTS idx_{emb_table}_field_doc_id ON {emb_table}(field, doc_id)",
    ]
    for sql in index_sqls:
        cur.execute(sql)


def _reset_all_policy_ids(conn):
    """policy_id 전체를 NULL로 초기화."""
    with conn.cursor() as cur:
        cur.execute("UPDATE documents SET policy_id = NULL")
    conn.commit()


def _has_higher_weight(conn, base_weight: float) -> bool:
    """해당 base_weight보다 높은 weight 문서가 존재하는지 확인."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM documents WHERE weight > %s LIMIT 1", (base_weight,))
        return cur.fetchone() is not None
def unify_policy_ids_after_grouping(conn, table="documents", id_col="id", parent_col="policy_id", verbose=True):
    """
    documents.policy_id 가 '부모 id' 를 가리키는 parent-pointer 체계일 때,
    모든 노드의 policy_id 를 최종 루트 id 로 일괄 갱신(경로 압축).

    - 루트(부모가 NULL)인 노드는 policy_id = 자기 id 로 채웁니다.
    - 순환(cycle) 방지: 재귀 CTE에 path 배열을 넣어 방문 노드 재방문을 차단합니다.
    """
    sql = f"""
    WITH RECURSIVE walk AS (
      -- 각 문서를 leaf로 시작
      SELECT d.{id_col} AS leaf, d.{id_col} AS cur_id, d.{parent_col} AS cur_parent, ARRAY[d.{id_col}] AS path
      FROM {table} d

      UNION ALL

      -- parent 포인터를 따라 위로 올라가며 루트 탐색
      SELECT w.leaf, p.{id_col} AS cur_id, p.{parent_col} AS cur_parent, w.path || p.{id_col}
      FROM walk w
      JOIN {table} p ON p.{id_col} = w.cur_parent
      WHERE w.cur_parent IS NOT NULL
        AND NOT p.{id_col} = ANY(w.path)  -- cycle 방지
    ),
    root_map AS (
      -- leaf 별 최종 루트: cur_parent 가 NULL 인 지점의 cur_id
      SELECT w.leaf AS {id_col},
             COALESCE(
               (SELECT w2.cur_id
                FROM walk w2
                WHERE w2.leaf = w.leaf AND w2.cur_parent IS NULL
                LIMIT 1),
               w.leaf  -- 안전장치: 루트를 못 찾으면 자기 자신
             ) AS root_id
      FROM walk w
      GROUP BY w.leaf
    )
    UPDATE {table} d
    SET {parent_col} = r.root_id
    FROM root_map r
    WHERE d.{id_col} = r.{id_col}
      AND d.{parent_col} IS DISTINCT FROM r.root_id;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        updated = cur.rowcount
    conn.commit()
    if verbose:
        print(f"[unify] updated rows: {updated}")

def assign_policy_ids(
    title_field: str = "title",
    similarity_threshold: float = 0.8,
    batch_size: int = 500,
    dry_run: bool = False,
    reset_all_on_start: bool = False,
    verbose: bool = True,
):
    """
    규칙(업데이트):
    1) 낮은 가중치부터 기준(base)으로 탐색 시작
    2) base.title 임베딩과 target.title 임베딩의 유사도(sim_new)가 threshold 이상이면 매칭 후보
    3) target.policy_id가 이미 존재하더라도, (현재 기준과의 유사도 sim_old)보다 sim_new가 클 때만 덮어쓰기
       - sim_old는 target.policy_id가 가리키는 문서의 title 임베딩과의 유사도를 의미
       - policy_id가 NULL이면 sim_old는 존재하지 않는 것으로 간주(=무조건 비교 대상)
    4) 동일 가중치 비교 금지: 오직 t.weight > base_weight 만 허용
    """
    emb_table = globals().get("_EMB_TABLE", "embeddings")
    emb_col   = globals().get("_EMB_COL", "embedding")

    dsn = _build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            _ensure_indexes(cur, emb_table)
        conn.commit()

        if reset_all_on_start:
            if verbose:
                print("[INIT] policy_id 전체 NULL 초기화 수행")
            _reset_all_policy_ids(conn)

        # base 후보: 제목 임베딩이 존재하는 문서만, weight 오름차순
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT d.id, d.weight
                  FROM documents d
                 WHERE EXISTS (
                        SELECT 1 FROM {emb_table} e
                         WHERE e.field = %s AND e.doc_id::bigint = d.id
                       )
                 ORDER BY d.weight ASC, d.id ASC
                """,
                (title_field,),
            )
            bases = cur.fetchall()

        if verbose:
            print(f"[RUN] base 문서 수: {len(bases)} (batch_size={batch_size}, dry_run={dry_run})")
            print("[MODE] 비교 규칙: 항상 t.weight > base_weight (동일 가중치 비교 금지).")
            print(f"[RULE] sim_new >= {similarity_threshold} 이고, (sim_old 없거나 sim_new > sim_old) 일 때만 매칭/덮어쓰기.")

        total_updates = 0

        for i in range(0, len(bases), batch_size):
            batch = bases[i : i + batch_size]

            for base_id, base_weight in batch:
                weight_comp = ">"  # ★ 동일 가중치 비교 금지

                if verbose:
                    print(f"  [BASE] id={base_id}, weight={base_weight} → weight 조건: t.weight {weight_comp} base_weight")

                with conn.cursor() as cur:
                    if dry_run:
                        # 후보 수 집계 (덮어쓰기 가능성까지 반영)
                        count_sql = f"""
                            WITH base AS (
                                SELECT e.{emb_col} AS base_emb
                                  FROM {emb_table} e
                                 WHERE e.field = %s AND e.doc_id::bigint = %s
                                 LIMIT 1
                            ),
                            cand AS (
                                SELECT t.id AS target_id,
                                       t.policy_id AS cur_pid,
                                       et.{emb_col} AS tgt_emb,
                                      ecur.{emb_col} AS cur_emb
                                  FROM documents t
                                  JOIN {emb_table} et
                                    ON et.doc_id::bigint = t.id AND et.field = %s
                                  LEFT JOIN {emb_table} ecur
                                    ON ecur.field = %s
                                   AND ecur.doc_id::bigint = t.policy_id
                                 WHERE t.weight {weight_comp} %s
                            ),
                            sims AS (
                                SELECT c.target_id,
                                       (1 - (c.tgt_emb <=> b.base_emb)) AS sim_new,
                                       CASE WHEN c.cur_emb IS NULL THEN NULL
                                            ELSE (1 - (c.tgt_emb <=> c.cur_emb))
                                       END AS sim_old
                                  FROM cand c
                                  CROSS JOIN base b
                            )
                            SELECT COUNT(*)
                              FROM sims
                             WHERE sim_new >= %s
                               AND (sim_old IS NULL OR sim_new > sim_old)
                        """
                        params = [
                            title_field,  # base 임베딩 field
                            base_id,
                            title_field,  # target 임베딩 field
                            title_field,  # 현재 policy_id가 가리키는 문서의 임베딩 field
                            base_weight,
                            similarity_threshold,
                        ]
                        cur.execute(count_sql, tuple(params))
                        cnt = cur.fetchone()[0]
                        if verbose:
                            print(f"    [DRY] 후보 {cnt}건 (sim_new >= {similarity_threshold} 이고 sim_new > sim_old)")
                    else:
                        # 실제 업데이트 (덮어쓰기 포함)
                        update_sql = f"""
                            WITH base AS (
                                SELECT d.id AS base_id, e.{emb_col} AS base_emb
                                  FROM documents d
                                  JOIN {emb_table} e
                                    ON e.doc_id::bigint = d.id AND e.field = %s
                                 WHERE d.id = %s
                                 LIMIT 1
                            ),
                            cand AS (
                                SELECT t.id AS target_id,
                                       t.policy_id AS cur_pid,
                                       et.{emb_col} AS tgt_emb,
                                       ecur.{emb_col} AS cur_emb
                                  FROM documents t
                                  JOIN {emb_table} et
                                    ON et.doc_id::bigint = t.id AND et.field = %s
                                  LEFT JOIN {emb_table} ecur
                                    ON ecur.field = %s
                                   AND ecur.doc_id::bigint = t.policy_id
                                 WHERE t.weight {weight_comp} %s
                                 FOR UPDATE OF t SKIP LOCKED
                            ),
                            sims AS (
                                SELECT c.target_id,
                                       (1 - (c.tgt_emb <=> b.base_emb)) AS sim_new,
                                       CASE WHEN c.cur_emb IS NULL THEN NULL
                                            ELSE (1 - (c.tgt_emb <=> c.cur_emb))
                                       END AS sim_old,
                                       b.base_id
                                  FROM cand c
                                  CROSS JOIN base b
                            ),
                            to_upd AS (
                                SELECT target_id, base_id
                                  FROM sims
                                 WHERE sim_new >= %s
                                   AND (sim_old IS NULL OR sim_new > sim_old)
                            )
                            UPDATE documents t
                               SET policy_id = u.base_id
                              FROM to_upd u
                             WHERE t.id = u.target_id
                            RETURNING t.id
                        """
                        params = [
                            title_field,  # base 임베딩 field
                            base_id,
                            title_field,  # target 임베딩 field
                            title_field,  # 현재 policy_id가 가리키는 문서의 임베딩 field
                            base_weight,
                            similarity_threshold,
                        ]
                        cur.execute(update_sql, tuple(params))
                        updated_rows = cur.fetchall()
                        total_updates += len(updated_rows)

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
        }

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    from argparse import ArgumentParser

    p = ArgumentParser()
    p.add_argument("--title-field", default="title")
    p.add_argument("--threshold", type=float, default=0.8)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--reset-all", action="store_true",
                   help="시작 시 documents.policy_id 전체 NULL 초기화")
    p.add_argument("--verbose", action="store_true", default=True)

    # 임베딩 테이블/컬럼명 오버라이드
    p.add_argument("--emb-table", default=os.getenv("EMB_TABLE", "embeddings"),
                   help="임베딩 테이블명 (기본: embeddings)")
    p.add_argument("--emb-col", default=os.getenv("EMB_COL", "embedding"),
                   help="임베딩 컬럼명 (기본: embedding)")

    # 2차 통일 패스 on/off 옵션 (원하면 끌 수 있게)
    p.add_argument("--unify", action="store_true", default=True,
                   help="1차 그루핑 후 policy_id를 루트로 통일 (default: on)")

    args = p.parse_args()

    globals()["_EMB_TABLE"] = args.emb_table
    globals()["_EMB_COL"] = args.emb_col

    # 1) 1차 그루핑 수행
    res = assign_policy_ids(
        title_field=args.title_field,
        similarity_threshold=args.threshold,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        reset_all_on_start=args.reset_all,
        verbose=args.verbose,
    )
    print(res)

    # 2) 2차 통일 패스 (별도 커넥션 열어서 실행)
    if args.unify and not args.dry_run:
        dsn = _build_dsn_from_env()
        conn = psycopg2.connect(dsn)
        try:
            unify_policy_ids_after_grouping(conn)
        finally:
            conn.close()

