# -*- coding: utf-8 -*-
"""
app/dao/db_user_utils.py

- users / profiles / collections / conversations / messages / conversation_embeddings
  쪽에 대한 공용 DB 유틸 함수 모음.
- 특징:
  * psycopg 커서(cur)를 인자로 받아서, persist_pipeline 등에서 하나의 트랜잭션 안에서 호출하기 쉽게 설계
  * 실제 스키마는 create_user_schema.py 기준 (profiles / conversations / messages / conversation_embeddings)
  * collections 스키마는 프로젝트마다 다를 수 있어, 최소한의 예시 형태로 작성 (TODO 표시)

주의:
  - 여기서는 "정책 DB(documents/embeddings)"는 건드리지 않는다. (조회 전용)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────────────
def _row_to_dict(cur, row) -> Dict[str, Any]:
    """psycopg 커서 + row → dict 형태로 변환"""
    if row is None:
        return {}
    cols = [d.name for d in cur.description]
    return {c: v for c, v in zip(cols, row)}


def _now_ts() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────
# 1. profiles 관련
#    - 스키마: create_user_schema.py 기준
# ─────────────────────────────────────────────────────
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


def get_profile_by_id(cur, profile_id: int) -> Optional[Dict[str, Any]]:
    """
    profiles.id 기준 단일 프로필 조회.
    """
    cur.execute(
        """
        SELECT id, user_id, name, birth_date, sex,
               residency_sgg_code, insurance_type,
               median_income_ratio, basic_benefit_type,
               disability_grade, ltci_grade,
               pregnant_or_postpartum12m, updated_at
          FROM profiles
         WHERE id = %s
        """,
        (profile_id,),
    )
    row = cur.fetchone()
    return _row_to_dict(cur, row) if row else None


def upsert_profile(cur, profile: Dict[str, Any]) -> int:
    """
    profile dict → profiles UPSERT.
    - profile["id"]가 있으면 해당 PK에 upsert
    - 없으면 INSERT 후 id 반환
    - 전달된 dict에서 PROFILE_COLUMNS에 해당하는 키만 사용

    반환: profile_id (BIGINT)
    """
    data = {k: profile.get(k) for k in PROFILE_COLUMNS if k in profile}
    # updated_at은 항상 현재 시각으로
    data["updated_at"] = _now_ts()

    # id 유무에 따라 INSERT / UPSERT 분기
    profile_id = data.get("id")

    if profile_id is None:
        # INSERT
        cols = [c for c in PROFILE_COLUMNS if c != "id" and c in data]
        vals = [data[c] for c in cols]
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        cur.execute(
            f"""
            INSERT INTO profiles ({col_list})
            VALUES ({placeholders})
            RETURNING id
            """,
            vals,
        )
        new_id = cur.fetchone()[0]
        return int(new_id)

    else:
        # UPSERT (id가 이미 있는 경우)
        cols = [c for c in PROFILE_COLUMNS if c != "id" and c in data]
        set_expr = ", ".join(f"{c} = %s" for c in cols)
        vals = [data[c] for c in cols] + [profile_id]
        cur.execute(
            f"""
            INSERT INTO profiles (id, {", ".join(cols)})
            VALUES (%s, {", ".join(["%s"] * len(cols))})
            ON CONFLICT (id) DO UPDATE SET
              {set_expr}
            """,
            [profile_id] + [data[c] for c in cols],
        )
        return int(profile_id)


# ─────────────────────────────────────────────────────
# 2. collections 관련
#    ⚠️ 스키마 프로젝트마다 다를 수 있어서 예시 형태로만 작성.
#    예시 스키마(가정):
#      collections(
#         profile_id BIGINT PRIMARY KEY,
#         data JSONB,
#         updated_at TIMESTAMPTZ
#      )
# ─────────────────────────────────────────────────────

def get_collection_by_profile(cur, profile_id: int) -> Optional[Dict[str, Any]]:
    """
    collections 테이블에서 profile_id 기준 단일 row 조회.

    ⚠️ 스키마 예시:
       SELECT profile_id, data, updated_at FROM collections ...
    실제 컬럼명이 다르면 이 함수를 수정해야 한다.
    """
    cur.execute(
        """
        SELECT profile_id, data, updated_at
          FROM collections
         WHERE profile_id = %s
        """,
        (profile_id,),
    )
    row = cur.fetchone()
    return _row_to_dict(cur, row) if row else None


def upsert_collection(cur, profile_id: int, coll_data: Dict[str, Any]) -> None:
    """
    collections UPSERT.

    ⚠️ 예시 스키마 기준:
       - profile_id: PK (1:1)
       - data: JSONB
       - updated_at: TIMESTAMPTZ
    """
    now = _now_ts()
    cur.execute(
        """
        INSERT INTO collections (profile_id, data, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (profile_id) DO UPDATE SET
          data = EXCLUDED.data,
          updated_at = EXCLUDED.updated_at
        """,
        (profile_id, coll_data, now),
    )


# ─────────────────────────────────────────────────────
# 3. conversations 관련
#    - 스키마: create_user_schema.py 기준
#      conversations(
#         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#         profile_id BIGINT UNIQUE,
#         started_at TIMESTAMPTZ,
#         ended_at TIMESTAMPTZ,
#         summary JSONB,
#         model_stats JSONB,
#         created_at TIMESTAMPTZ,
#         updated_at TIMESTAMPTZ
#      )
# ─────────────────────────────────────────────────────

def upsert_conversation(
    cur,
    profile_id: int,
    summary: Optional[Dict[str, Any]],
    model_stats: Optional[Dict[str, Any]],
    ended_at: Optional[datetime] = None,
) -> str:
    """
    profile_id 기준 conversations UPSERT.
    - profile_id UNIQUE라 가정 (1:1)
    - 없으면 INSERT, 있으면 UPDATE
    - summary/model_stats/ended_at 갱신

    반환: conversation_id (UUID string)
    """
    if ended_at is None:
        ended_at = _now_ts()

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
        (profile_id, summary, model_stats, ended_at),
    )
    conv_id = cur.fetchone()[0]
    return str(conv_id)


# ─────────────────────────────────────────────────────
# 4. messages 관련
#    - 스키마: create_user_schema.py 기준
#      messages(
#         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#         conversation_id UUID,
#         turn_index INT,
#         role TEXT,
#         content TEXT,
#         tool_name TEXT,
#         token_usage JSONB,
#         meta JSONB,
#         created_at TIMESTAMPTZ,
#         UNIQUE (conversation_id, turn_index, role)
#      )
# ─────────────────────────────────────────────────────

def bulk_insert_messages(
    cur,
    conversation_id: str,
    messages: Sequence[Dict[str, Any]],
    *,
    start_turn_index: int = 0,
) -> int:
    """
    messages 시퀀스를 한 번에 INSERT.
    - turn_index가 메시지 dict에 없으면 start_turn_index부터 순차 할당.
    - UNIQUE (conversation_id, turn_index, role) 제약이 있으므로
      같은 세션을 두 번 저장하는 경우에는 충돌 가능성 있음 → 호출 측에서 관리.

    반환: 실제 INSERT 시도한 행 수 (에러 시 예외 발생)
    """
    rows = []
    idx = start_turn_index
    for m in messages:
        role = m.get("role") or "user"
        content = m.get("content") or ""
        tool_name = m.get("meta", {}).get("tool_name") if m.get("meta") else None
        token_usage = m.get("meta", {}).get("token_usage") if m.get("meta") else None
        meta = m.get("meta") or {}
        created_at = m.get("created_at")
        if not created_at:
            created_at = _now_ts()
        else:
            # ISO 문자열이면 datetime으로 변환 시도
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
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
                token_usage,
                meta,
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
        """,
        rows,
    )
    return len(rows)


# ─────────────────────────────────────────────────────
# 5. conversation_embeddings 관련
#    - 스키마: create_user_schema.py 기준
#      conversation_embeddings(
#         conversation_id UUID,
#         chunk_id TEXT,
#         embedding VECTOR(D),
#         created_at TIMESTAMPTZ,
#         PRIMARY KEY (conversation_id, chunk_id)
#      )
# ─────────────────────────────────────────────────────

def bulk_insert_conversation_embeddings(
    cur,
    conversation_id: str,
    embeddings: Sequence[Dict[str, Any]],
) -> int:
    """
    conversation_embeddings에 벡터 여러 개를 한 번에 INSERT.
    - embeddings: 각 원소는 {"chunk_id": str, "embedding": List[float]} 형태를 기대.
    - pgvector 어댑터가 psycopg에 등록되어 있다고 가정.

    반환: INSERT한 행 수
    """
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
