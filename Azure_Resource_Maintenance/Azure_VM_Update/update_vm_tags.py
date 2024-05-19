import sys
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.identity import ManagedIdentityCredential

def update_vm_tags(subscription_id, resource_group_name, clustername):
    credential = ManagedIdentityCredential()
    compute_client = ComputeManagementClient(credential, subscription_id)
    
    # Get VM Info
    vms = compute_client.virtual_machines.list(resource_group_name)

    # Update Tag
    updated_tags = {
        "clustername": clustername
    }

    for vm in vms:
        print("Got VM:\n{}".format(vm))
    
        # Get the current tags of the VM
        current_tags = vm.tags

        # Update the tags with new values
        current_tags.update(updated_tags)

        compute_client.virtual_machines.begin_update(
            resource_group_name,
            vm.name,
            {
                "location": vm.location,
                "tags": current_tags
            }
        )
    
        print("Updated VM tags:\n{}".format(updated_tags))

if __name__ == "__main__":
    # Check if the required number of arguments are provided
    if len(sys.argv) != 4:
        print("Example for running the script: python3 update_vm_tag.py <subscription_id> <resource_group_name> <clustername>")
        sys.exit(1)

    # Get command line arguments
    subscription_id, resource_group_name, clustername = sys.argv[1:4]

    # Call the update function with the provided arguments
    update_vm_tags(subscription_id, resource_group_name, clustername)

# Don't forget to install the following packages before you run the scripts
# sudo pip3 install azure.mgmt
# sudo pip3 install --upgrade azure-identity azure-mgmt-compute
# sudo pip3 install --upgrade azure-mgmt-network
# sudo pip3 install --upgrade azure-identity azure-mgmt-compute azure-mgmt-resource