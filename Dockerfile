FROM python:3.12-slim

# 1001 사용자 생성
RUN groupadd -g 1001 ubuntu && useradd -u 1001 -g 1001 -m ubuntu

# 필요한 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    unzip \
    && curl https://rclone.org/install.sh | bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# uv 설치 및 의존성 동기화
COPY pyproject.toml uv.lock ./
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && /root/.local/bin/uv sync --frozen --no-cache

COPY . .

# 로그 및 데이터 디렉토리 권한 설정
RUN mkdir -p /app/log /app/downloads/pdf_archive_temp && chown -R ubuntu:ubuntu /app

USER ubuntu

CMD ["/root/.local/bin/uv", "run", "pdf_archiver_async.py"]
