# Multi-stage build: uv installs dependencies in the builder stage; the
# final image ships only the resulting virtual environment and source, a
# non-root user, and no secrets.

FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /uvx /bin/

WORKDIR /app

# Dependencies first, so an unchanged pyproject.toml/uv.lock reuses this
# layer even when only src/ changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.12-slim

RUN useradd --create-home --shell /usr/sbin/nologin worker

WORKDIR /app
COPY --from=builder /app/.venv ./.venv
COPY --from=builder /app/src ./src

ENV PATH="/app/.venv/bin:${PATH}"

# Logs are the observability transport here, and Python buffers stdout when it
# is not a terminal. Without this, records still sitting in the buffer are lost
# when the runtime sends SIGKILL -- exactly the moment they matter most.
ENV PYTHONUNBUFFERED=1

USER worker

# Exec form (not shell form) so the Python process is PID 1 and receives
# SIGTERM directly -- a shell-form CMD would put a shell at PID 1 and the
# signal would never reach the interpreter.
CMD ["python", "-m", "ingest.main"]
