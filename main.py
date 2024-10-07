import json

from deploy_module.deploy import DataScanManager
from deploy_module.rules_reader import RulesReader
import os



branch_name = os.getenv('BRANCH_NAME')
base_branch = os.getenv('BASE_BRANCH')
pr = os.getenv('PR')
print("*********************************************")
print(f"Validation of PR #{pr}: {branch_name} TO {base_branch}")
print("*********************************************")
manager = DataScanManager('dev')
manager.form_data_scans()
