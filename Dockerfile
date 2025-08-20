ARG MINIO_VERSION=RELEASE.2025-07-23T15-54-02Z
ARG PYTHON_VERSION=3.12

FROM minio/minio:${MINIO_VERSION} AS minio

FROM python:${PYTHON_VERSION} AS build
RUN curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | sh
COPY pyproject.toml uv.lock /srv/cwm-minio-api/
WORKDIR /srv/cwm-minio-api
RUN ~/.local/bin/uv export --no-emit-project > requirements.txt

FROM python:${PYTHON_VERSION}
# These should match the versions in .github/workflows/ci.yaml
ARG UV_VERSION=0.8.8
ARG MIGRATE_VERSION=v4.18.3
RUN curl -L https://github.com/golang-migrate/migrate/releases/download/${MIGRATE_VERSION}/migrate.linux-amd64.tar.gz | tar xvz &&\
    mv migrate /usr/local/bin/migrate &&\
    chmod +x /usr/local/bin/migrate
COPY --from=minio /usr/bin/mc /usr/local/bin/mc
RUN mkdir /srv/cwm-minio-api && adduser --system cwm-minio-api --home /srv/cwm-minio-api
COPY --from=build /srv/cwm-minio-api/requirements.txt /srv/cwm-minio-api/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r /srv/cwm-minio-api/requirements.txt
WORKDIR /srv/cwm-minio-api
COPY pyproject.toml gunicorn.conf.py docker_entrypoint.sh ./
COPY migrations ./migrations
COPY bin ./bin
COPY cwm_minio_api ./cwm_minio_api
RUN pip install --no-cache-dir --no-deps -e .
ARG VERSION=docker-build
RUN echo "VERSION = '${VERSION}'" > cwm_minio_api/version.py
RUN mkdir .mc && chown cwm-minio-api .mc
USER cwm-minio-api
CMD ["./docker_entrypoint.sh"]
