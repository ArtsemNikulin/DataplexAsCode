from deploy_module.deploy import DataScanManager
import os

branch_name = os.getenv('BRANCH_NAME').lower()
print("***********************")
print(f"Deploy for {branch_name.upper()} environment")
print("***********************")
manager = DataScanManager(env=branch_name)
manager.create_data_scan()