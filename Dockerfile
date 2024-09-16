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

RUN python -m pip install --upgrade pip

WORKDIR /stac_harvester_ingester
ADD LICENSE requirements.txt ./
ADD stac_harvester_ingester ./stac_harvester_ingester/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

ENTRYPOINT ["python", "-m", "stac_harvester_ingester"]
