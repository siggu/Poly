# -*- coding: utf-8 -*-
"""
app/dao/db_user_utils.py

✔ 통합 버전
- 기존 버전1: 트랜잭션(cur) 기반 유틸 (persist_pipeline 등에서 사용)
- 기존 버전2: 독립 connection 기반 프로필/컬렉션 조회

이 파일은 두 스타일을 모두 제공함:
  * get_profile_by_id(cur, id) / get_profile_by_id_conn(id)
  * get_collection_by_profile(cur, id) / get_collection_by_profile_conn(id)

주의:
  - 정책 DB(documents/embeddings)는 포함되지 않음 (조회 전용)
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime, timezone
import os
import json

from psycopg.types.json import Json
import psycopg
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgresql+psycopg://"):
    DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://", 1)


# ============================================================
# 공통 유틸
# ============================================================
def _row_to_dict(cur, row) -> Dict[str, Any]:
    """psycopg cursor row → dict"""
    if row is None:
        return {}
    cols = [d.name for d in cur.description]
    return {c: v for c, v in zip(cols, row)}


def _now_ts() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# 1) profiles
# ============================================================
PROFILE_COLUMNS: Tuple[str, ...] = (
    "id",
    "user_id",
    "name",
    "birth_date",
    "sex",
    "residency_sgg_code",
    "insurance_type",
    "median_income_ratio",
    "basic_benefit_type",
    "disability_grade",
    "ltci_grade",
    "pregnant_or_postpartum12m",
    "updated_at",
)


# -------------------------------
# 프로필 조회 (cur, transaction 기반)
# -------------------------------
def get_profile_by_id_cur(cur, profile_id: int) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT id, user_id, name, birth_date, sex,
           residency_sgg_code, insurance_type,
           median_income_ratio, basic_benefit_type,
           disability_grade, ltci_grade,
           pregnant_or_postpartum12m, updated_at
    FROM profiles
    WHERE id = %s
    """
    cur.execute(sql, (profile_id,))
    row = cur.fetchone()
    return _row_to_dict(cur, row) if row else None


def upsert_profile(cur, profile: Dict[str, Any]) -> int:
    data = {k: profile.get(k) for k in PROFILE_COLUMNS if k in profile}
    data["updated_at"] = _now_ts()

    profile_id = data.get("id")

    if profile_id is None:
        # INSERT
        cols = [c for c in PROFILE_COLUMNS if c != "id" and c in data]
        vals = [data[c] for c in cols]
        placeholders = ", ".join(["%s"] * len(vals))
        col_list = ", ".join(cols)

        cur.execute(
            f"""
            INSERT INTO profiles ({col_list})
            VALUES ({placeholders})
            RETURNING id
            """,
            vals,
        )
        return int(cur.fetchone()[0])

    # UPSERT
    cols_no_id = [c for c in PROFILE_COLUMNS if c != "id" and c in data]

    insert_cols = ["id"] + cols_no_id
    insert_values = [profile_id] + [data[c] for c in cols_no_id]
    insert_placeholders = ", ".join(["%s"] * len(insert_cols))

    set_expr = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols_no_id)

    cur.execute(
        f"""
        INSERT INTO profiles ({", ".join(insert_cols)})
        VALUES ({insert_placeholders})
        ON CONFLICT (id) DO UPDATE SET {set_expr}
        """,
        insert_values,
    )
    return int(profile_id)


# -------------------------------
# 프로필 조회 (독립 connection 버전)
# -------------------------------
def get_profile_by_id(profile_id: int) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT id, user_id, birth_date, sex, residency_sgg_code, insurance_type,
           median_income_ratio, basic_benefit_type, disability_grade,
           ltci_grade, pregnant_or_postpartum12m, updated_at
    FROM profiles
    WHERE id = %s
    LIMIT 1;
    """
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (profile_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "user_id": row[1],
                "birth_date": row[2],
                "sex": row[3],
                "residency_sgg_code": row[4],
                "insurance_type": row[5],
                "median_income_ratio": float(row[6]) if row[6] is not None else None,
                "basic_benefit_type": row[7],
                "disability_grade": row[8],
                "ltci_grade": row[9],
                "pregnant_or_postpartum12m": bool(row[10]) if row[10] is not None else None,
                "updated_at": row[11],
            }


# ============================================================
# 2) collections (triples)
# ============================================================

# -------------------------------
# 트랜잭션(cur) 기반 조회
# -------------------------------
def get_collection_by_profile_cur(cur, profile_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT id, profile_id, subject, predicate, object,
           code_system, code
    FROM collections
    WHERE profile_id = %s
    """
    cur.execute(sql, (profile_id,))
    rows = cur.fetchall()
    return [_row_to_dict(cur, r) for r in rows]


def upsert_collection(cur, profile_id: int, triples: List[Dict[str, Any]]) -> int:
    if not triples:
        return 0

    # 기존 키 불러오기
    cur.execute(
        """
        SELECT subject, predicate, object,
               COALESCE(code_system, ''), COALESCE(code, '')
        FROM collections
        WHERE profile_id = %s
        """,
        (profile_id,),
    )
    existing_keys = {
        (subj, pred, obj, cs, cd)
        for (subj, pred, obj, cs, cd) in cur.fetchall()
    }

    rows_to_insert = []
    for t in triples:
        subj = (t.get("subject") or "").strip()
        pred = (t.get("predicate") or "").strip()
        obj = (t.get("object") or "").strip()
        cs = (t.get("code_system") or "") or None
        cd = (t.get("code") or "") or None

        if not subj or not pred or not obj:
            continue

        key = (subj, pred, obj, cs or "", cd or "")
        if key in existing_keys:
            continue

        existing_keys.add(key)
        rows_to_insert.append((profile_id, subj, pred, obj, cs, cd))

    if not rows_to_insert:
        return 0

    cur.executemany(
        """
        INSERT INTO collections (profile_id, subject, predicate, object, code_system, code)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows_to_insert,
    )

    return len(rows_to_insert)


# -------------------------------
# 독립 connection 기반 조회
# -------------------------------
def get_collection_by_profile(profile_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT id, profile_id, subject, predicate, object,
           code_system, code, onset_date, end_date,
           negation, confidence, source_id, created_at
    FROM collections
    WHERE profile_id = %s
    ORDER BY created_at ASC;
    """
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (profile_id,))
            rows = cur.fetchall()

            out = []
            for r in rows:
                out.append({
                    "id": r[0],
                    "profile_id": r[1],
                    "subject": r[2],
                    "predicate": r[3],
                    "object": r[4],
                    "code_system": r[5],
                    "code": r[6],
                    "onset_date": r[7],
                    "end_date": r[8],
                    "negation": bool(r[9]) if r[9] is not None else False,
                    "confidence": float(r[10]) if r[10] is not None else None,
                    "source_id": r[11],
                    "created_at": r[12],
                })
            return out


# ============================================================
# 3) conversations / messages / embeddings
# ============================================================
# (기존 버전1 그대로 유지 — 트랜잭션 기반)
# ============================================================

def upsert_conversation(
    cur,
    profile_id: int,
    summary: Optional[Dict[str, Any]],
    model_stats: Optional[Dict[str, Any]],
    ended_at: Optional[datetime] = None,
) -> str:
    if ended_at is None:
        ended_at = _now_ts()

    summary_json = Json(summary) if summary is not None else None
    model_stats_json = Json(model_stats) if model_stats is not None else None

    cur.execute(
        """
        INSERT INTO conversations (profile_id, summary, model_stats, ended_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (profile_id) DO UPDATE SET
          summary = EXCLUDED.summary,
          model_stats = EXCLUDED.model_stats,
          ended_at = EXCLUDED.ended_at,
          updated_at = NOW()
        RETURNING id
        """,
        (profile_id, summary_json, model_stats_json, ended_at),
    )
    return str(cur.fetchone()[0])


def bulk_insert_messages(
    cur,
    conversation_id: str,
    messages: Sequence[Dict[str, Any]],
    *,
    start_turn_index: int = 0,
) -> int:
    """
    messages 시퀀스를 한 번에 INSERT.

    - messages: State에 쌓인 전체 메시지 목록 (user/assistant/tool 모두 포함)
    - start_turn_index: 기본 0.
      실제로는 (conversation_id, turn_index, role) UNIQUE 제약이 있어
      같은 턴이 이미 있으면 ON CONFLICT DO NOTHING 으로 스킵한다.
    """
    rows = []
    idx = start_turn_index
    for m in messages:
        role = m.get("role") or "user"
        content = m.get("content") or ""

        meta_dict = m.get("meta") or {}
        token_usage = meta_dict.get("token_usage")
        tool_name = meta_dict.get("tool_name")

        created_at = m.get("created_at") or _now_ts()

        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(
                    created_at.replace("Z", "+00:00")
                )
            except Exception:
                created_at = _now_ts()

        turn_index = m.get("turn_index", idx)
        idx += 1

        rows.append(
            (
                conversation_id,
                turn_index,
                role,
                content,
                tool_name,
                Json(token_usage) if token_usage else None,
                Json(meta_dict),
                created_at,
            )
        )

    if not rows:
        return 0

    cur.executemany(
        """
        INSERT INTO messages (
          conversation_id, turn_index, role,
          content, tool_name, token_usage, meta, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (conversation_id, turn_index, role) DO NOTHING
        """,
        rows,
    )
    return len(rows)


def bulk_insert_conversation_embeddings(
    cur,
    conversation_id: str,
    embeddings: Sequence[Dict[str, Any]],
) -> int:
    rows = []
    now = _now_ts()
    for e in embeddings:
        chunk_id = e.get("chunk_id")
        vec = e.get("embedding")
        if not chunk_id or vec is None:
            continue
        rows.append((conversation_id, chunk_id, vec, now))

    if not rows:
        return 0

    cur.executemany(
        """
        INSERT INTO conversation_embeddings (
          conversation_id, chunk_id, embedding, created_at
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (conversation_id, chunk_id) DO UPDATE SET
          embedding = EXCLUDED.embedding,
          created_at = EXCLUDED.created_at
        """,
        rows,
    )
    return len(rows)

# ============================================================
# 4) Retrieval Planner 전용 단일 호출 함수
# ============================================================

def fetch_profile_from_db(user_id: str) -> Optional[Dict[str, Any]]:
    """
    user_id로 최신 프로필 1개 조회.
    retrieval_planner에서 사용.
    """
    sql = """
        SELECT id, user_id, birth_date, sex, residency_sgg_code,
               insurance_type, median_income_ratio, basic_benefit_type,
               disability_grade, ltci_grade, pregnant_or_postpartum12m,
               updated_at
        FROM profiles
        WHERE user_id = %s
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1;
    """

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            row = cur.fetchone()
            if not row:
                return None

            return {
                "id": row[0],
                "user_id": row[1],
                "birth_date": row[2],
                "sex": row[3],
                "residency_sgg_code": row[4],
                "insurance_type": row[5],
                "median_income_ratio": float(row[6]) if row[6] is not None else None,
                "basic_benefit_type": row[7],
                "disability_grade": row[8],
                "ltci_grade": row[9],
                "pregnant_or_postpartum12m": bool(row[10]) if row[10] is not None else None,
                "updated_at": row[11],
            }


def fetch_collections_from_db(user_id: str) -> List[Dict[str, Any]]:
    """
    user_id → 최신 profile_id → 해당 컬렉션 목록 반환.
    retrieval_planner에서 사용.
    """
    # 먼저 최신 profile_id 조회
    pid_sql = """
        SELECT id FROM profiles
        WHERE user_id = %s
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1;
    """

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:

            cur.execute(pid_sql, (user_id,))
            row = cur.fetchone()
            if not row:
                return []
            profile_id = row[0]

            # 실제 컬렉션 조회
            sql = """
                SELECT id, profile_id, subject, predicate, object,
                       code_system, code, onset_date, end_date,
                       negation, confidence, source_id, created_at
                FROM collections
                WHERE profile_id = %s
                ORDER BY created_at ASC;
            """

            cur.execute(sql, (profile_id,))
            rows = cur.fetchall()

            results = []
            for r in rows:
                results.append({
                    "id": r[0],
                    "profile_id": r[1],
                    "subject": r[2],
                    "predicate": r[3],
                    "object": r[4],
                    "code_system": r[5],
                    "code": r[6],
                    "onset_date": r[7],
                    "end_date": r[8],
                    "negation": bool(r[9]) if r[9] else False,
                    "confidence": float(r[10]) if r[10] is not None else None,
                    "source_id": r[11],
                    "created_at": r[12],
                })

            return results