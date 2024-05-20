FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update --yes --quiet
RUN apt-get install --yes --quiet --no-install-recommends \
    openssh-client \
    procps \
    curl && \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl

RUN python -m pip install --upgrade pip

WORKDIR /stac_harvester_ingester
ADD LICENSE requirements.txt ./
ADD stac_harvester_ingester ./stac_harvester_ingester/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

ENTRYPOINT ["python", "-m", "stac_harvester_ingester"]
