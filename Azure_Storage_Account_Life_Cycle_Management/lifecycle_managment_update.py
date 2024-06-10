import azure_app_pass as password
from azure.identity import ClientSecretCredential
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import ManagementPolicy, ManagementPolicyRule, ManagementPolicyDefinition, ManagementPolicyAction, ManagementPolicyBaseBlob, DateAfterCreation, DateAfterModification ,ManagementPolicyFilter
import prefix_string as p
import json
class AzureStorageLifeCycleManagementUpdate:
    def __init__(self, resource_group_name, storage_account_name):
        self.tenant_id = password.tenant_id
        self.client_id = password.client_id
        self.client_secret = password.client_secret
        self.subscription_id = password.subscription_id
        self.resource_group_name = resource_group_name
        self.storage_account_name = storage_account_name
        self.client = None


    def create_client(self):
        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        self.client = StorageManagementClient(credential, self.subscription_id)
        
    
    def get_existing_policy(self):
        existing_policy = self.client.management_policies.get(
        resource_group_name = self.resource_group_name,
        account_name = self.storage_account_name,
        management_policy_name = "default"
        )
        return existing_policy
    
    # split the prefix path string to [[<=10 prefix path]]
    def create_prefix_path(self, s, blob_name):
        split_list = s.split(',')
        blob_name_with_prefix_path_list = [blob_name + '/' + prefix for prefix in split_list]
        return [blob_name_with_prefix_path_list[i : i + 10] for i in range(0, len(blob_name_with_prefix_path_list), 10)]

    # 0~90 days Cold，delete after 90 days, index used for create rule names
    def create_cold_to_delete_after_90_days_rule(self, prefix_list, index):
        rule_name = "coldToDeleteAfter90Days" + str(index)
        rule = ManagementPolicyRule(
            name = rule_name,
            enabled = True,
            type = "Lifecycle",
            definition = ManagementPolicyDefinition(
                actions = ManagementPolicyAction(
                    base_blob = ManagementPolicyBaseBlob(
                        tier_to_cold = DateAfterModification(days_after_modification_greater_than = 0),
                        delete = DateAfterModification(days_after_modification_greater_than = 90)
                    )
                ),
                filters=ManagementPolicyFilter(
                    blob_types = ["blockBlob"],
                    prefix_match = prefix_list  
                )
            )
        )
        return rule
    
    # combine rules with current cule and update it
    def update_rule(self, s, blob_name):
        #get exisiting policy
        self.create_client()
        existing_policy = self.get_existing_policy()
        if existing_policy.policy is None:
            existing_policy.policy = ManagementPolicy(rules=[])
        blob_name_with_prefix_path_list = self.create_prefix_path(s, blob_name)
        index = 1
        for prefix_path_list in blob_name_with_prefix_path_list:
            rule = self.create_cold_to_delete_after_90_days_rule(prefix_path_list, index)
            index += 1
            existing_policy.policy.rules.extend([rule])
        existing_policy_dict = existing_policy.as_dict()
        #rule prefix list for record
        with open('policy.json', 'w') as json_file:
            json.dump(existing_policy_dict, json_file, indent=2)
        
        self.client.management_policies.create_or_update(
            resource_group_name = self.resource_group_name,
            account_name = self.storage_account_name,
            properties = existing_policy,
            management_policy_name = "default"
        )
        
        

def main():
    azureStorageLifeCycleManagementUpdate = AzureStorageLifeCycleManagementUpdate("<Resource_Group_Name>", "<Storage_Account_Name>")
    azureStorageLifeCycleManagementUpdate.update_rule(p.prefix_string, "<Storage_Account_Name>")

    
if __name__ == '__main__':
    main()


