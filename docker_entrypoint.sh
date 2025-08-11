#!/usr/bin/env bash

set -euo pipefail


echo MINIO_TENANT_URL: "$MINIO_TENANT_URL"
echo MINIO_TENANT_ACCESSKEY: "$MINIO_TENANT_ACCESSKEY"
mc alias set cwm "$MINIO_TENANT_URL" "$MINIO_TENANT_ACCESSKEY" "$MINIO_TENANT_SECRETKEY"

gunicorn --print-config cwm_minio_api.app:app
exec gunicorn cwm_minio_api.app:app
