# app/dao/db_policy/dbuploader_policy.py
# -*- coding: utf-8 -*-
import os
import sys
import json
import argparse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import re

# 프로젝트 루트 경로 보정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    
import app.dao.utils_db as utils_db
    
# ─────────────────────────────────────────────────────────
# SentenceTransformers (dragonkue/BGE-m3-ko)
# ─────────────────────────────────────────────────────────
try:
    from sentence_transformers import SentenceTransformer
except Exception as e:
    print(f"[ERROR] sentence_transformers 로드 실패: {e}", file=sys.stderr)
    sys.exit(1)

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    utils_db.eprint("환경변수 DATABASE_URL이 필요합니다.")
    sys.exit(1)

def build_argparser():
    p = argparse.ArgumentParser(description="구조화 JSON을 documents/embeddings 테이블에 적재하는 로더")
    p.add_argument("--file","-f", default="app/output/ebogun.json",
                   help="적재할 JSON 파일 경로 (또는 __demo__ 로 데모 1건)")
    p.add_argument("--reset", choices=["none","truncate"], default="none",
                   help="로딩 전에 테이블 리셋 방식 (none|truncate). default: none")
    p.add_argument("--model", default="dragonkue/BGE-m3-ko",
                   help="임베딩 모델명 (default: dragonkue/BGE-m3-ko)")
    p.add_argument("--commit-every", type=int, default=50,
                   help="N개 문서마다 커밋 (default: 50)")
    return p

# ─────────────────────────────────────────────────────────
# 전처리 / 임베딩
# ─────────────────────────────────────────────────────────
_WS = re.compile(r"\s+")
def _normalize_space(s: str) -> str:
    return _WS.sub(" ", s).strip()

def preprocess_title(title: str) -> str:
    if not title: return ""
    t = str(title).replace("\n"," ").replace("\r"," ")
    return _normalize_space(t)

_ST_MODEL = None
def _load_st_model(model_name: str):
    global _ST_MODEL
    if _ST_MODEL is None:
        _ST_MODEL = SentenceTransformer(model_name)
    return _ST_MODEL

EMB_DIM = 1024  # pgvector(1024)
def _pad_or_truncate(vec, dim=EMB_DIM):
    if len(vec) == dim: return vec
    if len(vec) > dim:  return vec[:dim]
    return vec + [0.0]*(dim - len(vec))

def get_embedding(text: str, model_name: str):
    if not text or not str(text).strip():
        return None
    m = _load_st_model(model_name)
    v = m.encode(str(text).strip(), normalize_embeddings=False)
    v = v.tolist() if hasattr(v, "tolist") else list(v)
    v = _pad_or_truncate(v, EMB_DIM)
    return v

def _to_vector_literal(vec):
    # pgvector 입력 포맷: '[v1, v2, ...]'
    return "[" + ", ".join(f"{float(x):.8f}" for x in vec) + "]"

# ─────────────────────────────────────────────────────────
# 스키마 보장 (documents / embeddings)
# ─────────────────────────────────────────────────────────
CREATE_DOCUMENTS_SQL = """
CREATE TABLE IF NOT EXISTS documents (
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
"""

DOC_COLUMNS = {
    "title": "TEXT",
    "requirements": "TEXT",
    "benefits": "TEXT",
    "raw_text": "TEXT",
    "url": "TEXT",
    "policy_id": "BIGINT",
    "region": "TEXT",
    "sitename": "TEXT",
    "weight": "NUMERIC",
    "eval_target": "INTEGER",
    "eval_content": "INTEGER",
    "llm_reinforced": "BOOLEAN DEFAULT FALSE",
    "llm_reinforced_sources": "JSONB",
    "updated_at": "TIMESTAMPTZ DEFAULT NOW()",
}

def ensure_documents_schema(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id BIGSERIAL PRIMARY KEY,
        title TEXT,
        requirements TEXT,
        benefits TEXT,
        raw_text TEXT,
        url TEXT,
        policy_id BIGINT,
        region TEXT,
        sitename TEXT,
        weight NUMERIC,
        eval_target INTEGER,
        eval_content INTEGER,
        llm_reinforced BOOLEAN DEFAULT FALSE,
        llm_reinforced_sources JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_policy ON documents(policy_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_region ON documents(region);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url);")

    # 컬럼 보강(중복 추가 방지)
    for name, ddl in {
        "title": "TEXT",
        "requirements": "TEXT",
        "benefits": "TEXT",
        "raw_text": "TEXT",
        "url": "TEXT",
        "policy_id": "BIGINT",
        "region": "TEXT",
        "sitename": "TEXT",
        "weight": "NUMERIC",
        "eval_target": "INTEGER",
        "eval_content": "INTEGER",
        "llm_reinforced": "BOOLEAN DEFAULT FALSE",
        "llm_reinforced_sources": "JSONB",
        "updated_at": "TIMESTAMPTZ DEFAULT NOW()"
    }.items():
        cur.execute(f"ALTER TABLE documents ADD COLUMN IF NOT EXISTS {name} {ddl};")

    # 트리거 함수 생성(존재해도 덮어쓰기 OK)
    cur.execute("""
    CREATE OR REPLACE FUNCTION set_documents_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)

    # ⚠️ 여기서 'IF NOT EXISTS'를 쓰지 않습니다.
    cur.execute("SELECT 1 FROM pg_trigger WHERE tgname='trg_documents_updated_at';")
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("""
        CREATE TRIGGER trg_documents_updated_at
        BEFORE UPDATE ON documents
        FOR EACH ROW
        EXECUTE PROCEDURE set_documents_updated_at();
        """)

def ensure_embeddings_schema(cur):
    # vector 타입/확장 유무와 무관하게, 테이블은 표준 스키마로 생성
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS embeddings (
        id BIGSERIAL PRIMARY KEY,
        doc_id BIGINT REFERENCES documents(id) ON DELETE CASCADE,
        field TEXT,
        embedding vector(1024),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_doc ON embeddings(doc_id);")

def reset_tables(cur, mode: str):
    if mode == "truncate":
        cur.execute("TRUNCATE TABLE embeddings, documents RESTART IDENTITY CASCADE;")

# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────
def main():
    args = build_argparser().parse_args()
    json_path = args.file
    reset_mode = args.reset
    model_name = args.model
    commit_every = max(1, args.commit_every)

    # 입력 데이터 준비
    if json_path == "__demo__":
        data = [{
            "title": "서울시 65세 이상 독거어르신 방문건강관리",
            "support_target": "서울 거주 65세 이상 독거노인",
            "support_content": "간호사 방문 건강상담, 만성질환 관리, 보건소 연계",
            "raw_text": "서울시 방문건강관리 사업 안내...",
            "source_url": "https://seoul.go.kr/health/elderly-care",
            "region": "서울특별시",
            "eval_target": 8,
            "eval_content": 9
        }]
        print("🧪 데모 모드로 1건을 적재합니다.")
    else:
        if not os.path.exists(json_path):
            utils_db.eprint(f"입력 파일을 찾을 수 없습니다: {json_path}")
            sys.exit(1)
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                utils_db.eprint(f"JSON 파싱 오류: {e}")
                sys.exit(1)

    print(f"📡 Connecting to {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        # 스키마 보장
        ensure_documents_schema(cur)
        ensure_embeddings_schema(cur)
        if reset_mode != "none":
            reset_tables(cur, reset_mode)
            conn.commit()
            print(f"✅ 테이블 리셋 완료: {reset_mode}")

        inserted = 0
        total = len(data)

        for idx, item in enumerate(data, 1):
            title = item.get("title","")
            requirements = item.get("support_target","")
            benefits = item.get("support_content","")
            raw_text = item.get("raw_text","")
            url = item.get("source_url","")
            region = item.get("region","")

            eval_target = item.get("eval_target")     # 0~10 or None
            eval_content = item.get("eval_content")   # 0~10 or None

            policy_id = None
            sitename = utils_db.extract_sitename_from_url(url)
            weight = utils_db.get_weight(region, sitename) if hasattr(utils_db,"get_weight") else 0
            llm_reinforced = False
            llm_reinforced_sources = None

            # documents 삽입
            cur.execute("""
                INSERT INTO documents
                    (title, requirements, benefits, raw_text, url, policy_id,
                    region, sitename, weight, eval_target, eval_content,
                    llm_reinforced, llm_reinforced_sources)
                VALUES
                    (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s)
                RETURNING id;
            """, (
                title, requirements, benefits, raw_text, url, None,
                region, sitename, weight,
                eval_target, eval_content, llm_reinforced, llm_reinforced_sources
            ))
            doc_id = cur.fetchone()[0]
            # ✅ policy_id에 자신의 id 설정
            cur.execute(
                "UPDATE documents SET policy_id = %s WHERE id = %s;",
                (doc_id, doc_id)
            )

            # Embeddings 생성: title / requirements / benefits
            emb_rows = []
            title_norm = preprocess_title(title)
            for field, text_value in (("title", title_norm),
                                      ("requirements", requirements),
                                      ("benefits", benefits)):
                vec = get_embedding(text_value, model_name)
                if vec:
                    emb_rows.append((doc_id, field, _to_vector_literal(vec)))

            if emb_rows:
                # embeddings(doc_id, field, embedding vector(1024))
                execute_values(
                    cur,
                    "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                    emb_rows,
                    template="(%s, %s, %s::vector)"
                )

            inserted += 1
            if inserted % commit_every == 0:
                conn.commit()
                print(f"💾 {inserted}/{total}개 문서 커밋 완료")

            print(f"✅ Inserted document ({idx}/{total}): {title}")

        conn.commit()
        print(f"🎉 All data inserted successfully! 총 {inserted}건")

    except Exception as e:
        conn.rollback()
        utils_db.eprint(f"에러 발생으로 롤백했습니다: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
