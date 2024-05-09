FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update --yes --quiet
RUN apt-get install --yes --quiet --no-install-recommends \
    openssh-client \
    procps

RUN python -m pip install --upgrade pip

WORKDIR /harvest_transformer
ADD LICENSE requirements.txt ./
ADD harvest_transformer ./harvest_transformer/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

ENTRYPOINT ["python", "-m", "harvest_transformer"]
