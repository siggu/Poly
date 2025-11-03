# -*- coding: utf-8 -*-
import os
import sys
import json
import argparse
import psycopg2
from psycopg2.extras import execute_values
from openai import OpenAI
from dotenv import load_dotenv
import app.dao.utils_db as utils_db


# --------------------------------
# 1. 인자 파서
# --------------------------------
def build_argparser():
    p = argparse.ArgumentParser(
        description="ebogun.json 등을 읽어 documents/embeddings 테이블에 적재하는 로더"
    )
    p.add_argument(
        "--file", "-f",
        default="app/output/ebogun.json",
        help="적재할 JSON 파일 경로 (default: app/output/ebogun.json)"
    )
    p.add_argument(
        "--reset",
        choices=["none", "truncate"],
        default="none",
        help="로딩 전에 테이블 리셋 방식 (none|truncate). default: none"
    )
    p.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="임베딩 모델명 (default: text-embedding-3-small)"
    )
    p.add_argument(
        "--commit-every",
        type=int,
        default=50,
        help="N개 문서마다 커밋 (default: 50)"
    )
    return p


# --------------------------------
# 2. 환경 변수 로드
# --------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not DB_URL:
    utils_db.eprint("환경변수 DATABASE_URL이 필요합니다.")
    sys.exit(1)
if not OPENAI_API_KEY:
    utils_db.eprint("환경변수 OPENAI_API_KEY가 필요합니다.")
    sys.exit(1)

client = OpenAI(api_key=OPENAI_API_KEY)


# -------------------------------
# 4. 전처리 함수
# -------------------------------
def preprocess_title(title: str) -> str:
    """제목 임베딩 강건화를 위해 원문 + 띄어쓰기 제거 버전 병합"""
    if not title:
        return ""
    no_space = title.replace(" ", "")
    return f"{title.strip()} {no_space}"


# --------------------------------
# 5. 임베딩 함수
# --------------------------------
def get_embedding(text: str, model: str):
    if not text or text.strip() == "":
        return None
    resp = client.embeddings.create(
        model=model,
        input=text.replace("\n", " ")
    )
    return resp.data[0].embedding


# --------------------------------
# 6. 테이블 리셋
# --------------------------------
def reset_tables(cur, mode: str):
    """
    mode == 'truncate' 인 경우:
      - 외래키를 고려해 embeddings → documents 순으로 TRUNCATE
      - RESTART IDENTITY + CASCADE
    """
    if mode == "truncate":
        cur.execute("TRUNCATE TABLE embeddings, documents RESTART IDENTITY CASCADE;")


# --------------------------------
# 7. 메인 로직
# --------------------------------
def main():
    args = build_argparser().parse_args()

    json_path = args.file
    reset_mode = args.reset
    model_name = args.model
    commit_every = max(1, args.commit_every)

    if not os.path.exists(json_path):
        utils_db.eprint(f"입력 파일을 찾을 수 없습니다: {json_path}")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            utils_db.eprint(f"JSON 파싱 오류: {e}")
            sys.exit(1)

    # DB 연결
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        # 선택적 테이블 리셋
        if reset_mode != "none":
            reset_tables(cur, reset_mode)
            conn.commit()
            print(f"✅ 테이블 리셋 완료: {reset_mode}")

        inserted = 0

        for idx, item in enumerate(data, 1):
            title = item.get("title", "")
            requirements = item.get("support_target", "")
            benefits = item.get("support_content", "")
            raw_text = item.get("raw_text", "")
            url = item.get("source_url", "")
            policy_id = None
            region = item.get("region", "")
            sitename = utils_db.extract_sitename_from_url(url)
            weight = utils_db.get_weight(region, url) if hasattr(utils_db, "get_weight") else 0

            # documents 삽입 (플레이스홀더 9개로 수정)
            cur.execute(
                """
                INSERT INTO documents (title, requirements, benefits, raw_text, url, policy_id, region, sitename, weight)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (title, requirements, benefits, raw_text, url, policy_id, region, sitename, weight)
            )
            doc_id = cur.fetchone()[0]

            # --- title 전처리 후 임베딩 ---
            title_modified = preprocess_title(title)

            # 각 필드별 임베딩 생성
            emb_rows = []
            for fname, text_value in (
                ("title", title_modified),
                ("requirements", requirements),
                ("benefits", benefits),
            ):
                vec = get_embedding(text_value, model_name)
                if vec:
                    # float 리스트 그대로 넣어 double precision[] 컬럼에 저장
                    emb_rows.append((doc_id, fname, vec))

            # 일괄 삽입
            if emb_rows:
                execute_values(
                    cur,
                    "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                    emb_rows,
                    template="(%s, %s, %s)"
                )

            inserted += 1

            # 주기적으로 커밋
            if inserted % commit_every == 0:
                conn.commit()
                print(f"💾 {inserted}개 문서 커밋 완료")

            print(f"✅ Inserted document ({idx}/{len(data)}): {title}")

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
