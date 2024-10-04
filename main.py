import json
from deploy_module.deploy import DataScanManager

x = DataScanManager('dev').create_data_scans()

# print(json.dumps(x, indent=4))

