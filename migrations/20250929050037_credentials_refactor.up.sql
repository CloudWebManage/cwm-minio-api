alter table buckets drop column access_key;

alter table instances add column access_key text;

create table bucket_credentials (
    instance_id text,
    bucket_name text,
    access_key text not null,
    permission_read boolean not null default false,
    permission_write boolean not null default false,
    permission_delete boolean not null default false
);

create index idx_bucket_credentials_instance_id_bucket_name on bucket_credentials (instance_id, bucket_name);

create table access_keys (
    access_key text primary key
);

drop index idx_buckets_instance_id_name;

create unique index buckets_pkey on buckets (name);
