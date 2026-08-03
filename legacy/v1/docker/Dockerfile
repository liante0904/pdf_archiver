FROM python:3.12-slim

# 1. 필수 시스템 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    unzip \
    && curl https://rclone.org/install.sh | bash \
    && rm -rf /var/lib/apt/lists/*

# 2. uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# 3. 의존성 설치 (캐시 최적화)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

# 4. 소스 코드 복사
COPY . .

# 5. 권한 설정
RUN groupadd -g 1001 ubuntu && useradd -u 1001 -g 1001 -m ubuntu \
    && mkdir -p /app/log /app/downloads/pdf_archive_temp \
    && chown -R ubuntu:ubuntu /app

USER ubuntu

# 6. 실행 (uv를 통해 실행)
CMD ["uv", "run", "pdf_archiver_async.py"]
