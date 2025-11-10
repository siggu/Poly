# -*- coding: utf-8 -*-
"""
dbreinforcer.py (schema-adjusted)
- documents(requirements/benefits)이 빈약한 문서를, 같은 policy_id 문서들을 모아 LLM으로 통합 요약하여 보강.

테이블 스키마(요청 반영):
documents:  title, requirements, benefits, raw_text, url, policy_id, sitename, weight
embeddings: id, doc_id, field, embedding, created_at

동작 개요:
1) 대상 문서 선택: policy_id가 있고 requirements/benefits가 비었거나 너무 짧은 문서
2) 같은 policy_id의 다른 문서를 weight 우선으로 모아 소스로 사용
3) LLM이 '지원 대상(support_target), 지원 내용(support_content)' 두 필드를 JSON으로 통합 생성
4) 대상 문서의 requirements/benefits 갱신 + (없다면) llm_reinforced, llm_reinforced_sources 컬럼 생성/기록
5) 해당 문서의 embeddings에서 field in ('requirements','benefits')만 삭제 후 재생성

필수 환경변수:
- DATABASE_URL 또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
- OPENAI_API_KEY
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
from dotenv import load_dotenv
from openai import OpenAI


# ──────────────────────────────────────────────────────────────────────────────
# DSN Builder
# ──────────────────────────────────────────────────────────────────────────────
def build_dsn_from_env() -> str:
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
        raise ValueError("DATABASE_URL 또는 (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)가 필요합니다.")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"


# ──────────────────────────────────────────────────────────────────────────────
# SQLs (스키마 반영)
# ──────────────────────────────────────────────────────────────────────────────
# llm 관련 보조 컬럼(없을 수 있어 자동 추가)
ALTER_DOCUMENTS_SQL = """
ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS llm_reinforced BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS llm_reinforced_sources JSONB,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
"""

# 대상 문서 선별: policy_id는 있어야 함
TARGET_SELECT_BASE = """
SELECT
    d.ctid,  -- 내부 식별 위해(동일 id 미가정), 실제 업데이트/삭제는 PK 없으므로 id 대신 WHERE로 id 대신 primary key가 없다면 ctid 사용할 수 없음;
             -- 단, 보통 id PK가 있다고 가정하는 편이 안전. 여기서는 id가 있다고 가정.
    d.id,
    d.title,
    d.requirements,
    d.benefits,
    d.raw_text,
    d.url,
    d.policy_id,
    d.sitename,
    d.weight
FROM documents d
WHERE d.policy_id IS NOT NULL
"""

# 빈약 기준(길이/공백)
TARGET_FILTER_MISSING = """
AND (
    COALESCE(NULLIF(TRIM(d.requirements), ''), '') = '' OR length(d.requirements) < %(min_len_req)s
    OR
    COALESCE(NULLIF(TRIM(d.benefits), ''), '') = '' OR length(d.benefits) < %(min_len_ben)s
)
"""

TARGET_FILTER_POLICYID = "AND d.policy_id = %(policy_id)s\n"
TARGET_FILTER_DOCID    = "AND d.id = %(doc_id)s\n"
TARGET_ORDER_LIMIT     = "ORDER BY d.id ASC LIMIT %(limit)s\n"

# 같은 policy_id의 소스 문서(대상 제외), weight 우선 → 텍스트 길이 보조
SOURCES_SELECT = """
SELECT
    s.id, s.title, s.requirements, s.benefits, s.raw_text, s.url, s.sitename, s.weight
FROM documents s
WHERE s.policy_id = %(policy_id)s
  AND s.id <> %(target_id)s
  AND (
        (s.requirements IS NOT NULL AND length(TRIM(s.requirements)) >= 1)
     OR (s.benefits    IS NOT NULL AND length(TRIM(s.benefits))    >= 1)
     OR (s.raw_text    IS NOT NULL AND length(TRIM(s.raw_text))    >= 1)
  )
  AND (s.llm_reinforced IS DISTINCT FROM TRUE)
ORDER BY s.weight DESC,
         GREATEST(length(COALESCE(s.requirements,'')),
                  length(COALESCE(s.benefits,'')),
                  length(COALESCE(s.raw_text,''))) DESC
LIMIT %(max_sources)s
"""

# 보강 결과 반영 (region 필드 없음 → 지역 맞춤 제거)
UPDATE_TARGET_SQL = """
UPDATE documents
SET requirements = %(requirements)s,
    benefits     = %(benefits)s,
    raw_text     = CASE
                     WHEN raw_text IS NULL OR raw_text = '' THEN %(reinforce_note)s
                     ELSE raw_text || %(reinforce_note)s
                   END,
    llm_reinforced = TRUE,
    llm_reinforced_sources = %(provenance)s,
    updated_at = NOW()
WHERE id = %(doc_id)s
"""

# embeddings 테이블: (id, doc_id, field, embedding, created_at)
DELETE_OLD_EMB = "DELETE FROM embeddings WHERE doc_id = %(doc_id)s AND field IN ('requirements','benefits')"
INSERT_EMB = "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s"


# ──────────────────────────────────────────────────────────────────────────────
# LLM Prompt (지역 맞춤 문구 제거, weight 우선 요약 유지)
# ──────────────────────────────────────────────────────────────────────────────
def build_prompt(title: str, sources: List[Dict[str, Any]]) -> Tuple[str, str]:
    system = """너는 한국의 보건/복지 사업 문서를 통합·정제하는 전문가야.
여러 사이트에서 게시된 동일 정책의 문서를 모아 '지원 대상'과 '지원 내용'을 정확하고 간결하게 통합 요약해.
반드시 다음 규칙을 지켜:
1) 신뢰도 가중치 우선: weight가 높은 문서의 서술을 우선 채택하고, 내용이 충돌하면 더 높은 weight를 따른다.
2) 꾸미지 말 것: 출처들 어디에도 없는 수치/금액/기간/절차를 새로 만들지 마라. 불명확하면 '정보 없음'으로 남겨라.
3) 표기 정리: 문장형 요약, 핵심 항목은 불릿으로 정리해도 좋다. 중복/군더더기는 제거한다.
4) 현행성: 반복 서술은 더 일반적이고 최신으로 보이는 표현을 취하되, 근거가 불명확하면 보수적으로.
5) 구조: 'support_target'(지원 대상), 'support_content'(지원 내용) 두 필드만 작성한다. 각 3~6줄 이내.
6) 기관·연락처·신청창구는 특정 지자체 고유 명칭일 경우 일반명으로 표현한다(예: '보건소', '구청 담당부서').
7) **지역 일반화:** 동작구민/영등포구민/서초구민 등 특정 ‘OO구 + (구민|주민|거주자)’ 표현은 모두 '지역구민'으로 표기한다.
   또한 '영등포구·서초구·강동구 주민'처럼 여러 구를 나열한 경우에도 '지역구민'으로 통일한다."""

    blocks = []
    for i, s in enumerate(sources, 1):
        meta = {
            "id": s["id"],
            "title": s.get("title") or "",
            "sitename": s.get("sitename") or "",
            "weight": s.get("weight"),
            "url": s.get("url") or ""
        }
        req = (s.get("requirements") or "").strip()
        ben = (s.get("benefits") or "").strip()
        raw = (s.get("raw_text") or "").strip()
        if len(raw) > 3000:
            raw = raw[:3000] + "\n[... 이하 생략 ...]"
        block = f"""[SOURCE #{i}]
META: {json.dumps(meta, ensure_ascii=False)}
REQUIREMENTS:
{req if req else "(없음)"}
BENEFITS:
{ben if ben else "(없음)"}
RAW:
{raw if raw else "(없음)"}"""
        blocks.append(block)

    user = f"""아래는 같은 정책(policy)으로 분류된 서로 다른 사이트의 원문이야.
정책 제목: {title}

소스들:
-------------------------------------------------------------------------------
{chr(10).join(blocks)}
-------------------------------------------------------------------------------

요청: 위 규칙에 따라 통합·정제해 **JSON으로만** 반환해.
JSON 스키마: {{"support_target": "...", "support_content": "..."}}"""

    return system, user


# ──────────────────────────────────────────────────────────────────────────────
# Embedding
# ──────────────────────────────────────────────────────────────────────────────
def get_embedding(client: OpenAI, text: str, model: str) -> Optional[List[float]]:
    if not text or not text.strip():
        return None
    resp = client.embeddings.create(model=model, input=text.replace("\n", " "))
    return resp.data[0].embedding


# ──────────────────────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────────────────────
def reinforce_once(
    conn,
    client: OpenAI,
    emb_model: str,
    llm_model: str,
    target_row: Dict[str, Any],
    max_sources: int,
    dry_run: bool = False,
) -> bool:
    """단일 대상 문서 보강 처리"""
    target_id = target_row["id"]
    policy_id = target_row["policy_id"]
    title     = target_row["title"]

    # 1) 소스 수집
    with conn.cursor() as cur:
        cur.execute(SOURCES_SELECT, {"policy_id": policy_id, "target_id": target_id, "max_sources": max_sources})
        sources = [
            {
                "id": r[0], "title": r[1], "requirements": r[2], "benefits": r[3],
                "raw_text": r[4], "url": r[5], "sitename": r[6], "weight": r[7]
            }
            for r in cur.fetchall()
        ]

    if not sources:
        print(f"  · 대상 {target_id}: 같은 policy_id 소스 없음 → 건너뜀")
        return False

    # 2) LLM 통합
    system, user = build_prompt(title=title, sources=sources)
    try:
        comp = client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
        )
        payload = json.loads(comp.choices[0].message.content)
        new_req = (payload.get("support_target") or "").strip()
        new_ben = (payload.get("support_content") or "").strip()

    except Exception as e:
        print(f"  · 대상 {target_id}: LLM 실패 - {e}")
        return False

    # 3) DB 업데이트 + provenance
    provenance = {
        "policy_id": policy_id,
        "source_doc_ids": [s["id"] for s in sources],
        "weights": {str(s["id"]): s["weight"] for s in sources},
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "llm_model": llm_model,
    }
    note_text = f"""\n\n[LLM Reinforced at {datetime.utcnow().isoformat()}Z]
- policy_id: {policy_id}
- sources: {", ".join([str(s["id"]) for s in sources])}
"""

    # --- [NEW] dry-run 프리뷰 추가 부분 ---
    if dry_run:
        print("  · (dry-run) Reinforce Preview")
        print("    ──────────────────────────────────────────────────────")
        print(f"    [Doc] id={target_id} | policy_id={policy_id} | title={title}")
        print(f"    [Sources used] {', '.join(str(s['id']) for s in sources)}")
        print("    [Top weights]  " + ", ".join(f"{s['id']}:{s['weight']}" for s in sources[:5]))
        print("    [Original Requirements]")
        print("    -------------------------------------------------------")
        print((target_row.get('requirements') or '').strip()[:400] or '(없음)')
        print("    -------------------------------------------------------")
        print("    [Reinforced Requirements]")
        print("    -------------------------------------------------------")
        print(new_req if len(new_req) < 800 else new_req[:800] + '\n... (trimmed) ...')
        print("    -------------------------------------------------------")
        print("    [Original Benefits]")
        print("    -------------------------------------------------------")
        print((target_row.get('benefits') or '').strip()[:400] or '(없음)')
        print("    -------------------------------------------------------")
        print("    [Reinforced Benefits]")
        print("    -------------------------------------------------------")
        print(new_ben if len(new_ben) < 800 else new_ben[:800] + '\n... (trimmed) ...')
        print("    -------------------------------------------------------")
        print("    [Provenance(JSON)]")
        print(json.dumps({
            "policy_id": policy_id,
            "source_doc_ids": [s["id"] for s in sources],
            "weights": {str(s["id"]): s["weight"] for s in sources},
            "llm_model": llm_model
        }, ensure_ascii=False))
        print("    ──────────────────────────────────────────────────────")
        return True

    with conn.cursor() as cur:
        cur.execute(
            UPDATE_TARGET_SQL,
            {
                "requirements": new_req,
                "benefits": new_ben,
                "reinforce_note": note_text,
                "provenance": Json(provenance),
                "doc_id": target_id,
            },
        )

        # 4) 임베딩 재계산 (embeddings: doc_id, field, embedding)
        cur.execute(DELETE_OLD_EMB, {"doc_id": target_id})
        emb_rows = []
        for field_name, text in (("requirements", new_req), ("benefits", new_ben)):
            vec = get_embedding(client, text, emb_model)
            if vec:
                emb_rows.append((target_id, field_name, vec))
        if emb_rows:
            execute_values(cur, INSERT_EMB, emb_rows, template="(%s, %s, %s)")

    print(f"  · 대상 {target_id}: 업데이트 완료 (req/ben + embeddings + llm_reinforced=true)")
    return True


def select_targets(conn, args) -> List[Dict[str, Any]]:
    sql = [TARGET_SELECT_BASE]
    params = {
        "min_len_req": args.min_len_req,
        "min_len_ben": args.min_len_ben,
        "limit": args.limit,
    }
    if args.only_missing:
        sql.append(TARGET_FILTER_MISSING)
    if args.policy_id:
        sql.append(TARGET_FILTER_POLICYID)
        params["policy_id"] = args.policy_id
    if args.doc_id:
        sql.append(TARGET_FILTER_DOCID)
        params["doc_id"] = args.doc_id
    sql.append(TARGET_ORDER_LIMIT)

    cols = ["ctid", "id", "title", "requirements", "benefits", "raw_text", "url", "policy_id", "sitename", "weight"]
    with conn.cursor() as cur:
        cur.execute("".join(sql), params)
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def main():
    p = argparse.ArgumentParser(description="LLM로 documents(requirements/benefits)를 보강하는 Reinforcer")
    p.add_argument("--policy-id", type=int, help="특정 policy_id만 처리")
    p.add_argument("--doc-id", type=int, help="특정 document id만 처리")
    p.add_argument("--only-missing", action="store_true", default=True, help="요약이 빈약한 문서만 처리")
    p.add_argument("--min-len-req", type=int, default=15, help="requirements 최소 길이 기준")
    p.add_argument("--min-len-ben", type=int, default=15, help="benefits 최소 길이 기준")
    p.add_argument("--max-sources", type=int, default=5, help="통합에 사용할 최대 소스 개수")
    p.add_argument("--limit", type=int, default=100, help="대상 문서 최대 개수")
    p.add_argument("--model", default="gpt-4o-mini", help="LLM 모델명")
    p.add_argument("--embed-model", default="text-embedding-3-small", help="임베딩 모델명")
    p.add_argument("--dry-run", action="store_true", help="DB 갱신 없이 시뮬레이션")
    p.add_argument("--no-add-columns", action="store_true", help="llm_reinforced 관련 컬럼 추가 생략")
    args = p.parse_args()

    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("환경변수 OPENAI_API_KEY가 필요합니다.", file=sys.stderr)
        sys.exit(1)

    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    client = OpenAI(api_key=api_key)

    try:
        with conn.cursor() as cur:
            if not args.no_add_columns:
                cur.execute(ALTER_DOCUMENTS_SQL)
        conn.commit()

        targets = select_targets(conn, args)
        if not targets:
            print("처리 대상 문서가 없습니다.")
            return

        print(f"대상 문서 수: {len(targets)}")
        ok = 0
        for i, t in enumerate(targets, 1):
            print(f"[{i}/{len(targets)}] ID={t['id']} | policy_id={t['policy_id']} | title={t['title']}")
            if reinforce_once(conn, client, args.embed_model, args.model, t, args.max_sources, dry_run=args.dry_run):
                ok += 1
            conn.commit()

        print(f"완료: {ok}/{len(targets)} 성공")

    except Exception as e:
        conn.rollback()
        print(f"에러 발생: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
