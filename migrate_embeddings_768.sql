-- ========================================
-- 임베딩 차원 변경: 1024 → 768
-- 모델: BGE-m3-ko → jhgan/ko-sroberta-multitask
-- ========================================

-- 1. 기존 embeddings 테이블 백업
CREATE TABLE IF NOT EXISTS embeddings_backup_1024 AS
SELECT * FROM embeddings;

-- 백업 확인
SELECT COUNT(*) as backup_count FROM embeddings_backup_1024;

-- 2. 기존 embeddings 테이블 삭제
DROP TABLE IF EXISTS embeddings CASCADE;

-- 3. 새 차원(768)으로 embeddings 테이블 재생성
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    doc_id INT NOT NULL,
    field VARCHAR(50) NOT NULL,
    embedding vector(768),  -- 1024 → 768 변경
    UNIQUE(doc_id, field)
);

-- 4. 인덱스 재생성
-- B-tree 인덱스
CREATE INDEX IF NOT EXISTS idx_embeddings_doc_field
ON embeddings (doc_id, field);

-- pgvector ivfflat 인덱스 (벡터 검색 최적화)
CREATE INDEX IF NOT EXISTS idx_embeddings_vector_ivfflat
ON embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- 5. 확인
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'embeddings';

-- 완료 메시지
SELECT '✅ 스키마 변경 완료! 이제 임베딩을 재생성해야 합니다.' as status;

-- ========================================
-- 롤백 방법 (문제 발생 시):
-- ========================================
-- DROP TABLE IF EXISTS embeddings CASCADE;
-- ALTER TABLE embeddings_backup_1024 RENAME TO embeddings;
-- CREATE INDEX IF NOT EXISTS idx_embeddings_doc_field ON embeddings (doc_id, field);
-- CREATE INDEX IF NOT EXISTS idx_embeddings_vector_ivfflat ON embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
