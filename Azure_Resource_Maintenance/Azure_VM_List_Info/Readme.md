# Production version of list_vm_info script

## Environment setting:
python 3.9.0  

pandas 2.1.4  

## Notes:
Deploy the script on any VM that can access to the relevant Azure resources

Modified api_version from '2018-08-01' to '2018-11-01' (Not sure why, this worked though) 

Dataframe storage method changed due to removal of "append" attribute in Pandas 2.0  

Setup and schedule "load_data_from_stage_to_prod" sql job to load the data from staging table to the prod table as a hourly task