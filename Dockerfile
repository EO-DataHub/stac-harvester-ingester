FROM ghcr.io/astral-sh/uv:0.9.12 AS uv
FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
    && apt-get install --yes --quiet --no-install-recommends \
    openssh-client \
    procps \
    git

# Install uv
COPY --from=uv /uv /uvx /bin/

WORKDIR /stac_harvester_ingester

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

# Copy application code
ADD LICENSE stac_harvester_ingester ./stac_harvester_ingester/

# Install the project

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "--no-project", "python", "-m", "stac_harvester_ingester"]
