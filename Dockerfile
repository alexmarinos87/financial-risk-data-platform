FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY pyproject.toml README.md requirements.lock ./
COPY src ./src
COPY config ./config
COPY sql ./sql

RUN PIP_CONSTRAINT=requirements.lock python -m pip install . \
    && python -m pip check

USER 10001:10001

CMD ["python", "-m", "src.orchestration.run_pipeline"]
