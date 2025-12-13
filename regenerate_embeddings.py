# -*- coding: utf-8 -*-
"""
regenerate_embeddings.py
- documents 테이블은 유지하고 embeddings만 재생성
- 환경 변수 EMBEDDING_MODEL 사용 (기본값: jhgan/ko-sroberta-multitask)
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

# 환경 변수 로드
load_dotenv()

# 설정
EMB_DIM = 768
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "jhgan/ko-sroberta-multitask")
DB_URL = os.getenv("DATABASE_URL")

if not DB_URL:
    print("❌ DATABASE_URL 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

def build_vector_literal(vec, dim=EMB_DIM):
    """파이썬 list[float] -> pgvector 문자열 리터럴"""
    if not vec:
        return None
    if len(vec) > dim:
        vec = vec[:dim]
    elif len(vec) < dim:
        vec = list(vec) + [0.0] * (dim - len(vec))
    parts = (f"{float(x):.7f}" for x in vec)
    return "[" + ",".join(parts) + "]"


def get_embedding(text, model):
    """텍스트를 임베딩 벡터로 변환"""
    if not text or not str(text).strip():
        return None
    vec = model.encode(str(text).strip(), normalize_embeddings=True)
    return vec.tolist() if hasattr(vec, "tolist") else list(vec)


def main():
    print(f"📌 임베딩 모델: {EMBEDDING_MODEL}")
    print(f"📌 임베딩 차원: {EMB_DIM}")
    print(f"📌 DB URL: {DB_URL[:50]}...")

    # 모델 로드
    print(f"\n⏳ 모델 로딩 중: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)
    print(f"✅ 모델 로드 완료!")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        # 1. embeddings 테이블 비우기
        print("\n⏳ embeddings 테이블 비우는 중...")
        cur.execute("TRUNCATE TABLE embeddings;")
        conn.commit()
        print("✅ embeddings 테이블 비움")

        # 2. documents 개수 확인
        cur.execute("SELECT COUNT(*) FROM documents;")
        total_docs = cur.fetchone()[0]
        print(f"\n📊 총 {total_docs}개 문서의 임베딩 생성 시작")

        if total_docs == 0:
            print("⚠️  documents 테이블이 비어있습니다.")
            return

        # 3. documents 읽어서 임베딩 생성
        cur.execute("""
            SELECT id, title, requirements, benefits
            FROM documents
            ORDER BY id
        """)

        batch_size = 100
        batch = []
        processed = 0
        bar_length = 40

        print("\n진행 상황:")
        print("=" * 60)

        for row in cur.fetchall():
            doc_id, title, requirements, benefits = row

            # title, requirements, benefits에 대해 임베딩 생성
            for field_name, text in [
                ("title", title),
                ("requirements", requirements),
                ("benefits", benefits),
            ]:
                if text and str(text).strip():
                    vec = get_embedding(text, model)
                    if vec:
                        lit = build_vector_literal(vec, EMB_DIM)
                        if lit:
                            batch.append((doc_id, field_name, lit))

            processed += 1

            # 배치 단위로 삽입
            if len(batch) >= batch_size:
                execute_values(
                    cur,
                    "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                    batch,
                    template="(%s, %s, %s::vector)",
                )
                conn.commit()
                batch = []

            # 진행률 표시
            percent = (processed / total_docs) * 100
            filled = int(bar_length * percent / 100)
            bar = "█" * filled + "-" * (bar_length - filled)
            sys.stdout.write(
                f"\r|{bar}| {percent:6.2f}% ({processed}/{total_docs})"
            )
            sys.stdout.flush()

        # 남은 배치 처리
        if batch:
            execute_values(
                cur,
                "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                batch,
                template="(%s, %s, %s::vector)",
            )
            conn.commit()

        print(f"\n\n✅ 완료! {total_docs}개 문서의 임베딩 생성 완료")

        # 4. 결과 확인
        cur.execute("SELECT COUNT(*) FROM embeddings;")
        total_embeddings = cur.fetchone()[0]
        print(f"📊 생성된 임베딩: {total_embeddings}개")

        # 5. 샘플 확인
        cur.execute("""
            SELECT d.id, d.title, vector_dims(e.embedding) as dim
            FROM documents d
            JOIN embeddings e ON e.doc_id = d.id
            WHERE e.field = 'title'
            LIMIT 3
        """)
        print("\n📋 샘플 확인:")
        for doc_id, title, dim in cur.fetchall():
            print(f"  ID {doc_id}: {title[:50]}... (dim: {dim})")

    except Exception as e:
        conn.rollback()
        print(f"\n\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
