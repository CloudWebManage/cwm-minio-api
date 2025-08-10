#!/usr/bin/env bash

set -euo pipefail

gunicorn --print-config cwm_minio_api.app:app
exec gunicorn cwm_minio_api.app:app
