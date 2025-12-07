#!/bin/bash
set -e

echo "🚀 Starting Poly Application..."

# 백엔드 실행 (백그라운드)
echo "📡 Starting FastAPI backend on port 8000..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

# 백엔드가 준비될 때까지 대기
echo "⏳ Waiting for backend to be ready..."
for i in {1..30}; do
  if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ Backend is ready!"
    break
  fi
  echo "Waiting... ($i/30)"
  sleep 2
done

# 프론트엔드 실행 (포그라운드)
echo "🎨 Starting Streamlit frontend on port 8501..."
streamlit run app/frontend/app.py \
  --server.port=8501 \
  --server.address=0.0.0.0 \
  --server.headless=true \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false
