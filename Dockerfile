FROM ghcr.io/astral-sh/uv:alpine3.22 AS base

ENV PYTHONUNBUFFERED=1

RUN apk update
RUN apk add libpq libpq-dev gcc g++ musl-dev rust cargo libffi-dev cmake binutils make py3-pyarrow

WORKDIR /app

# Install requirements
COPY pyproject.toml .
COPY uv.lock .

RUN uv venv --system-site-packages
RUN uv run uv sync

# copy application files
COPY . .

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]