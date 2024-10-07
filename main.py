import json

from deploy_module.deploy import DataScanManager
from deploy_module.rules_reader import RulesReader
import os
from deploy_module.git_changes import get_changed_files


branch_name = os.getenv('BRANCH_NAME')
base_branch = os.getenv('BASE_BRANCH')
pr = os.getenv('PR')
print("*********************************************")
print(f"Validation of PR #{pr}: {branch_name} TO {base_branch}")
print("*********************************************")
# manager = DataScanManager('dev')
# manager.form_data_scans()
x = get_changed_files()
z = [changed_rules for changed_rules in x if changed_rules.name == 'rules.yaml']
# z = RulesReader(x).get_datasets_with_rules()

print(z)