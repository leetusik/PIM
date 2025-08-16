# pyproject.toml에 명시된 파이썬 버전을 사용합니다.
FROM python:3.12.6-slim

WORKDIR /app

# =================================================================
# uv 설치 (공식 권장 방식)
# 1. curl을 사용하여 uv 설치 스크립트를 다운로드하고 실행합니다.
# 2. uv를 /usr/local/bin에 설치하여 시스템 전역에서 사용할 수 있게 합니다.
# =================================================================
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# uv가 설치된 경로를 PATH에 추가합니다.
ENV PATH="/root/.local/bin:${PATH}"

# 의존성 파일만 먼저 복사 (Docker 캐싱 최적화)
COPY pyproject.toml ./

# 가상환경 생성 후 의존성 설치
RUN uv venv /opt/venv --python 3.12
ENV PATH="/opt/venv/bin:$PATH"
# HTTP 타임아웃 설정
ENV UV_HTTP_TIMEOUT=120

# 애플리케이션 소스 코드 복사
COPY . .

# 프로젝트와 의존성 설치
RUN uv pip install -e .

# entrypoint 스크립트에 실행 권한 부여
RUN chmod +x /app/entrypoint.sh