# CWM MinIO API

## Usage

Once you run the app, see the API documentation at the app domain under path `/docs`

## Configuration

Configuration values are set as env vars in `.env` file.

You can see the full list of available configuration options in the following files:

* App configuration values: [cwm_minio_api/config.py](cwm_minio_api/config.py)
* Web server configuration values: [gunicorn_conf.py](gunicorn_conf.py)

## Prometheus

The API exposes Prometheus metrics at `/metrics`.

If you run the API with multiple Gunicorn workers, set `PROMETHEUS_MULTIPROC_DIR` to a writable directory to aggregate metrics across workers (see `gunicorn.conf.py`).

## Local Development

Prerequisites:

* Python 3.12
* [uv](https://pypi.org/project/uv/)
* [Migrate](https://github.com/golang-migrate/migrate/tree/master/cmd/migrate#installation)
* Docker
* Minio MC
  * Get the version from the Dockerfile `MINIO_VERSION`
  * Run the docker image: `docker run --rm --name minio -d minio/minio:<MINIO_VERSION> server /srv`
  * Copy the `mc` binary: `docker cp minio:/usr/bin/mc /usr/local/bin/mc`
  * Create Minio cwm profile connected to relevant cluster: `mc alias set cwm MINIO_URL USER PASSWORD`

Install:

```
uv sync --all-extras
```

Set configuration values in `.env` file (See Configuration section above for details)

Start the DB and apply migrations:

```
docker compose up -d db
bin/migrate.sh up
```

Run the CLI:

```shell
uv run cwm-minio-api --help
```

Run the web app:

```
uv run uvicorn cwm_minio_api.app:app --reload --factory
```

Access the API Docs at http://localhost:8000/docs


## DB Migrations

Apply all migrations:

```
bin/migrate.sh up
```

Create a new migration:

```
bin/migrate_create.sh descriptive_migration_name
```

Migrations management:

```
bin/migrate.sh --help
```

## Tests

To run tests, start the DB container:

```
docker compose up -d db
```

Run tests with:

```
uv run pytest
```

## Load Tests

Start Redis container:

```
docker run -d --name redis -p 6379:6379 redis:8
```

Start locust:

```
uv sync --all-extras
locust -f cwm_minio_api/load_tests/locustfile.py
```

Start tests from the web UI:

http://localhost:8089
