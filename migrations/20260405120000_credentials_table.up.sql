create table credentials (
    instance_id text not null,
    access_key text primary key
);

create index idx_credentials_instance_id on credentials (instance_id);

insert into credentials (instance_id, access_key)
select distinct instance_id, access_key
from bucket_credentials;

create unique index idx_bucket_credentials_instance_bucket_access_key
on bucket_credentials (instance_id, bucket_name, access_key);
