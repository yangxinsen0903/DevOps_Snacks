--register partitions
alter table <db_schema>.vm_info_check_h add if not exists partition(dt='${date_partition}');

insert overwrite table <db_schema>.vm_info_check_hi
partition(dt='${date_partition}')
select
  vm_name string,
  instance_size string,
  availability_zone string,
  instance_group_name string,
  instance_type string,
  subnet string
from <db_schema>.vm_info_check_hi where dt='${date_partition}';