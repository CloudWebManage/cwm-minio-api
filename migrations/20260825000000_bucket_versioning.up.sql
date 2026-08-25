alter table buckets
    add column versioning_enabled boolean not null default false,
    add column versioning_ilm_rule_id text;
