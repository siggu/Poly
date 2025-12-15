# -*- coding: utf-8 -*-
"""
regenerate_embeddings.py
- documents 테이블은 유지하고 embeddings만 재생성
- OpenAI text-embedding-3-small 모델 사용 (1536차원)
"""

import os
import sys
import time
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from openai import OpenAI

# 환경 변수 로드
load_dotenv()

# 설정
EMB_DIM = 1536  # OpenAI text-embedding-3-small
EMBEDDING_MODEL = "text-embedding-3-small"
DB_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not DB_URL:
    print("❌ DATABASE_URL 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

if not OPENAI_API_KEY:
    print("❌ OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
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


def get_embedding(text, client):
    """OpenAI API를 사용하여 텍스트를 임베딩 벡터로 변환"""
    if not text or not str(text).strip():
        return None

    try:
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=str(text).strip()
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"\n⚠️  임베딩 생성 오류: {e}")
        return None


def get_embeddings_batch(texts, client):
    """배치로 여러 텍스트의 임베딩을 한 번에 생성 (API 호출 최적화)"""
    if not texts:
        return []

    # 빈 텍스트 필터링
    valid_texts = [(i, text) for i, text in enumerate(texts) if text and str(text).strip()]

    if not valid_texts:
        return [None] * len(texts)

    try:
        # OpenAI API는 배치 처리 지원
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=[str(text).strip() for _, text in valid_texts]
        )

        # 결과를 원래 순서에 맞게 매핑
        results = [None] * len(texts)
        for j, (i, _) in enumerate(valid_texts):
            results[i] = response.data[j].embedding

        return results
    except Exception as e:
        print(f"\n⚠️  배치 임베딩 생성 오류: {e}")
        return [None] * len(texts)


def main():
    print(f"📌 임베딩 모델: OpenAI {EMBEDDING_MODEL}")
    print(f"📌 임베딩 차원: {EMB_DIM}")
    print(f"📌 DB URL: {DB_URL[:50]}...")

    # OpenAI 클라이언트 초기화
    print(f"\n⏳ OpenAI 클라이언트 초기화 중...")
    client = OpenAI(api_key=OPENAI_API_KEY)
    print(f"✅ OpenAI 클라이언트 준비 완료!")

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

        all_docs = cur.fetchall()
        batch_size = 50  # OpenAI API 배치 크기 (너무 크면 타임아웃 위험)
        db_batch = []
        processed = 0
        bar_length = 40

        print("\n진행 상황:")
        print("=" * 60)

        # API 배치 처리를 위한 임시 저장소
        api_batch_texts = []
        api_batch_meta = []  # (doc_id, field_name)

        for row in all_docs:
            doc_id, title, requirements, benefits = row

            # title, requirements, benefits를 배치에 추가
            for field_name, text in [
                ("title", title),
                ("requirements", requirements),
                ("benefits", benefits),
            ]:
                if text and str(text).strip():
                    api_batch_texts.append(text)
                    api_batch_meta.append((doc_id, field_name))

            # API 배치가 충분히 쌓이면 한 번에 처리
            if len(api_batch_texts) >= batch_size:
                embeddings = get_embeddings_batch(api_batch_texts, client)

                for (doc_id, field_name), vec in zip(api_batch_meta, embeddings):
                    if vec:
                        lit = build_vector_literal(vec, EMB_DIM)
                        if lit:
                            db_batch.append((doc_id, field_name, lit))

                # DB에 삽입
                if db_batch:
                    execute_values(
                        cur,
                        "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                        db_batch,
                        template="(%s, %s, %s::vector)",
                    )
                    conn.commit()
                    db_batch = []

                # 배치 초기화
                api_batch_texts = []
                api_batch_meta = []

                # Rate limiting
                time.sleep(0.1)

            processed += 1

            # 진행률 표시
            percent = (processed / total_docs) * 100
            filled = int(bar_length * percent / 100)
            bar = "█" * filled + "-" * (bar_length - filled)
            sys.stdout.write(
                f"\r|{bar}| {percent:6.2f}% ({processed}/{total_docs})"
            )
            sys.stdout.flush()

        # 남은 API 배치 처리
        if api_batch_texts:
            embeddings = get_embeddings_batch(api_batch_texts, client)

            for (doc_id, field_name), vec in zip(api_batch_meta, embeddings):
                if vec:
                    lit = build_vector_literal(vec, EMB_DIM)
                    if lit:
                        db_batch.append((doc_id, field_name, lit))

        # 남은 DB 배치 처리
        if db_batch:
            execute_values(
                cur,
                "INSERT INTO embeddings (doc_id, field, embedding) VALUES %s",
                db_batch,
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
