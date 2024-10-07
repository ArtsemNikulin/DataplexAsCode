from deploy_module.git_changes import get_changed_files
from deploy_module.rules_reader import RulesReader
from deploy_module.deploy import DataScanManager
import os

branch_name = os.getenv('BRANCH_NAME')
base_branch = os.getenv('BASE_BRANCH')
pr = os.getenv('PR')

print("*********************************************")
print(f"Getting GIT changes")
print("*********************************************")

changed_files = get_changed_files()
changed_rules = [changed_rules for changed_rules in changed_files if changed_rules.name == 'rules.yaml']
print(changed_rules)

if len(changed_rules) == 0:
    print("There are no formalized DataScans, since no changes in Rules")
    print(f"The following changes are detected: {changed_files}")

else:
    print("*********************************************")
    print(f"Validation of PR #{pr}: {branch_name} TO {base_branch}")
    print("*********************************************")

    datasets_with_rules = RulesReader(changed_rules).get_datasets_with_rules()
    DataScanManager(env='dev', datasets_with_rules=datasets_with_rules).create_data_scans(validate=True)



