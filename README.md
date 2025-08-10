# CWM MinIO API

## Usage

Once you run the app, see the API documentation at the app domain under path `/docs`

## Configuration

Configuration values are set as env vars in `.env` file.

You can see the full list of available configuration options in the following files:

* App configuration values: [cwm_minio_api/config.py](cwm_minio_api/config.py)
* Web server configuration values: [gunicorn_conf.py](gunicorn_conf.py)

## Local Development

Prerequisites:

* Python 3.12
* [uv](https://pypi.org/project/uv/)
* [Migrate](https://github.com/golang-migrate/migrate/tree/master/cmd/migrate#installation)

Install:

```
uv sync
```

Set configuration values in `.env` file (See Configuration section above for details)

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
