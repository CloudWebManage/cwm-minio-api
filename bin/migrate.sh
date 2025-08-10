#!/usr/bin/env bash

migrate -path migrations -database "${MIGRATE_DATABASE_URL:-postgres://postgres:123456@localhost:5432/postgres?sslmode=disable}" "$@"
