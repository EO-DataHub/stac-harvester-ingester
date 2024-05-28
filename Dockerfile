FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update --yes --quiet
RUN apt-get install --yes --quiet --no-install-recommends \
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
