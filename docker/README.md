# Docker 배포 가이드

이 폴더에는 Poly 프로젝트를 Docker로 배포하기 위한 모든 파일이 포함되어 있습니다.

## 📁 파일 구조

```
docker/
├── Dockerfile              # 기본 Docker 이미지
├── Dockerfile.prod         # 프로덕션용 멀티스테이지 빌드
├── docker-compose.yml      # Docker Compose 설정
├── docker-entrypoint.sh    # 컨테이너 시작 스크립트
├── render.yaml             # Render.com 배포 설정
└── README.md               # 이 파일
```

---

## 🚀 빠른 시작

### 1. 로컬에서 실행

프로젝트 루트 디렉토리에서:

```bash
# 빌드
docker-compose -f docker/docker-compose.yml build

# 실행
docker-compose -f docker/docker-compose.yml up

# 또는 백그라운드 실행
docker-compose -f docker/docker-compose.yml up -d
```

### 2. 브라우저 접속

- **Streamlit UI**: http://localhost:8501
- **FastAPI API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

---

## 🛠️ Docker 명령어

### 컨테이너 관리

```bash
# 시작
docker-compose -f docker/docker-compose.yml up -d

# 중지
docker-compose -f docker/docker-compose.yml down

# 재시작
docker-compose -f docker/docker-compose.yml restart

# 로그 보기
docker-compose -f docker/docker-compose.yml logs -f

# 상태 확인
docker-compose -f docker/docker-compose.yml ps

# 컨테이너 내부 접속
docker-compose -f docker/docker-compose.yml exec poly-app bash
```

### 이미지 관리

```bash
# 이미지 다시 빌드 (코드 변경 시)
docker-compose -f docker/docker-compose.yml build --no-cache

# 이미지 삭제
docker-compose -f docker/docker-compose.yml down --rmi all

# 볼륨까지 모두 삭제
docker-compose -f docker/docker-compose.yml down -v --rmi all
```

---

## 🌐 클라우드 배포

### Railway.app (가장 간단)

1. [Railway.app](https://railway.app) 가입
2. "New Project" → "Deploy from GitHub repo"
3. Repository 선택
4. Environment Variables 설정:
   ```
   OPENAI_API_KEY=sk-proj-...
   GEMINI_API_KEY=AIza...
   DB_HOST=152.67.192.180
   DB_PORT=5432
   DB_NAME=database
   DB_USER=admin
   DB_PASSWORD=1234
   ```
5. 자동 배포 완료!

### Render.com

1. [Render.com](https://render.com) 가입
2. "New" → "Blueprint"
3. GitHub 연동 후 Repository 선택
4. `docker/render.yaml` 감지 후 자동 배포

### Oracle Cloud (수동 배포)

```bash
# 1. SSH 접속
ssh user@152.67.192.180

# 2. Docker 설치
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 3. Docker Compose 설치
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 4. 프로젝트 클론
git clone https://github.com/siggu/Poly.git
cd Poly

# 5. .env 파일 생성 (로컬에서 복사)

# 6. 실행
docker-compose -f docker/docker-compose.yml up -d

# 7. 로그 확인
docker-compose -f docker/docker-compose.yml logs -f
```

---

## 📊 프로덕션 최적화

### 멀티스테이지 빌드 사용

이미지 크기를 줄이려면 `Dockerfile.prod` 사용:

```bash
docker build -f docker/Dockerfile.prod -t poly-app:prod .
```

### 환경변수 분리

프로덕션 환경에서는 `.env` 파일 대신 환경변수 직접 설정:

```bash
docker run -e OPENAI_API_KEY=xxx -e GEMINI_API_KEY=yyy poly-app:prod
```

---

## 🐛 트러블슈팅

### "Backend is not ready" 에러

**원인**: FastAPI 시작이 느림

**해결**: `docker-entrypoint.sh`의 대기 시간 증가
```bash
for i in {1..60}; do  # 30 → 60
```

### DB 연결 실패

**확인**:
```bash
docker-compose -f docker/docker-compose.yml exec poly-app python check_db.py
```

**해결**: `.env`의 `DB_HOST` 확인

### 포트 충돌

**원인**: 8000 또는 8501 포트가 이미 사용 중

**해결**: `docker-compose.yml`에서 포트 변경
```yaml
ports:
  - "8080:8000"  # 8000 → 8080
  - "8502:8501"  # 8501 → 8502
```

---

## 📝 참고

- **컨텍스트**: 프로젝트 루트 (`..`)
- **Dockerfile 위치**: `docker/Dockerfile`
- **볼륨 마운트**: 개발 시에만 활성화 (프로덕션에서는 제거)

---

## ⚠️ 보안 주의사항

1. `.env` 파일을 GitHub에 푸시하지 마세요
2. API 키는 환경변수로 관리하세요
3. 프로덕션에서는 HTTPS 사용하세요
