ALTER TABLE buckets ADD COLUMN blocked boolean NOT NULL DEFAULT false;
ALTER TABLE instances ADD COLUMN blocked boolean NOT NULL DEFAULT false;
ALTER TABLE instances DROP COLUMN max_size_gb;
