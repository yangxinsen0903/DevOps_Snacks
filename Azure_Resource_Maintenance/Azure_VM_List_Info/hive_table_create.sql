---create hive table for incremental data(staging)

create external table <db_schema>.vm_info_check_h (
    vm_name string,
    instance_size string,
    availability_zone string,
    instance_group_name string,
    instance_type string,
    subnet string
)
partitioned by (dt string)
row format delimited 
fields terminated by ','
lines terminated by '\n'
stored as textfile
location 'table_path_under_cloud_storage_service'

---create hive table for full load data(prod)

create table  <db_schema>.vm_info_check_hi (
  vm_name string,
  instance_size string,
  availability_zone string,
  instance_group_name string,
  instance_type string,
  subnet string
)
partitioned by (dt string comment 'dt')
stored as orc