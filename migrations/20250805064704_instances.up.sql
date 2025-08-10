create table instances (
    id text not null primary key,
    max_size_gb integer not null
);

create index idx_instances_id on instances (id);
