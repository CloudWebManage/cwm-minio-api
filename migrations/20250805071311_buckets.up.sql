create table buckets (
    instance_id text not null,
    name text not null,
    public boolean not null,
    access_key text not null
);

create index idx_buckets_instance_id_name on buckets (instance_id, name);
