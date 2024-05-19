import sys
import pandas as pd
from azure.identity import ManagedIdentityCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta

def get_subnet_info(network_client, resource_group, network_interface_id):
    # Extract the network interface name from the ID
    network_interface_name = network_interface_id.split("/")[-1]

    try:
        # Get the network interface details
        network_interface = network_client.network_interfaces.get(resource_group, network_interface_name)

        # Check if IP configurations are present
        if network_interface.ip_configurations:
            ip_configuration = network_interface.ip_configurations[0]

            # Check if subnet is present in the IP configuration
            if ip_configuration.subnet:
                subnet_parts = ip_configuration.subnet.id.split("/")
                subnet = subnet_parts[-1]
                return subnet
        # If subnet information is not found, return "N/A"
        return "N/A"

    except Exception as e:
        print(f"Error getting subnet info for {network_interface_name} in {resource_group}: {str(e)}")
        return "N/A"

def list_vm_info(subscription_id):
    credential = ManagedIdentityCredential()

    # Use ResourceManagementClient to get resource groups
    resource_client = ResourceManagementClient(credential, subscription_id)
    
    resource_groups = resource_client.resource_groups.list()

    compute_client = ComputeManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id, api_version='2018-11-01')

    # Create an empty DataFrame
    #df = pd.DataFrame(columns=["VM Name", "Instance Size", "Availability Zone", "SDP Group Name", "Instance Type", "Subnet", "dt"])
    
    # Initialize a list to collect VM info
    vm_info_list = []

    for resource_group in resource_groups:
        # Print the resource group name for debugging
        print(f"Processing resource group: {resource_group.name}")

        # Get VM Info in the current resource group
        vms = compute_client.virtual_machines.list(resource_group.name)
        for vm in vms:
            # Get the current tags of the VM
            current_tags = vm.tags

            # Check if tags are present
            if current_tags:

                # Assuming the VM has only one network interface, you might need to modify
                # this part if the VM can have multiple interfaces
                if vm.network_profile and vm.network_profile.network_interfaces:
                    network_interface_reference = vm.network_profile.network_interfaces[0]
                    network_interface_id = network_interface_reference.id

                    # Get the subnet information
                    subnet = get_subnet_info(network_client, resource_group.name, network_interface_id)
                else:
                    subnet = "N/A"

                purchase_type = current_tags.get("sdp-purchasetype", "N/A")
                if purchase_type == "1":
                    purchase_type_str = "On-Demand"
                elif purchase_type == "2":
                    purchase_type_str = "Spot"
                else:
                    purchase_type_str = "N/A"

                # Get the current date&hour in the format yyyymmddhh
                current_date = datetime.now().strftime("%Y%m%d%H")

                # Get node role by VM tag
                sdp_group_name = current_tags.get("sdp-groupname") or current_tags.get("sdp-role")

                # Get node role from VM name if both tags not exist
                if sdp_group_name == None:
                    vm_name = vm.name.lower()

                    if "-amb-" in vm_name:
                        sdp_group_name = "ambari"
                    elif "-mst-" in vm_name:
                        sdp_group_name = "master"
                    elif "-cor-" in vm_name:
                        sdp_group_name = "core"
                    else:
                        sdp_group_name = "task"
                

                vm_info = {
                    "VM Name": vm.name,
                    "Instance Size": vm.hardware_profile.vm_size,
                    "Availability Zone": current_tags.get("OpsCloud Zone", "N/A"),
                    "SDP Group Name": sdp_group_name,
                    "Instance Type": purchase_type_str,
                    "Subnet": subnet,
                    "dt": current_date
                }

                # Append the VM info to the DataFrame
                #df = df.append(vm_info, ignore_index=True)

                # Add the vm_info dictionary to the list
                vm_info_list.append(vm_info)
            else:
                current_tags = "N/A"

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(vm_info_list, columns=["VM Name", "Instance Size", "Availability Zone", "SDP Group Name", "Instance Type", "Subnet", "dt"])

    return df

def upload_to_azure(local_file, storage_account_name, container_name, folder_name):
    credential = ManagedIdentityCredential()

    # Use BlobServiceClient to get Azure storage resource
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)

    # Interact with azure storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{folder_name}/{local_file}")

    # Upload csv file to Azure storage
    with open(local_file, "rb") as data_file:
        blob_client.upload_blob(data_file, overwrite=False)

    print(f"{local_file} uploaded to {container_name}/{folder_name} successfully.")

if __name__ == "__main__":
    # Check if the required number of arguments are provided
    if len(sys.argv) != 2:
        print("Usage: python3 python_script.py <subscription_id>")
        sys.exit(1)

    # Get command line arguments
    subscription_id = sys.argv[1]

    # Call the function with the provided arguments
    result_df = list_vm_info(subscription_id)

    # Export DataFrame to CSV file
    file_name = "vm_info.csv"
    result_df.to_csv(file_name, index=False, header=False)
    # print("Result exported to vm_info.csv")

    # Set Azure storage account details
    storage_account_name = "<your_storage_account_name>"
    container_name = "<container_path_under_storage_account>"
    #folder_name = datetime.now().strftime("dt=%Y%m%d%H")
    # Current VM is in UTC timezone, convert it to CST
    folder_name = (datetime.now()+timedelta(hours=8)).strftime("dt=%Y%m%d%H")


    # Execute uploading
    upload_to_azure(file_name, storage_account_name, container_name, folder_name)

