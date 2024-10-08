from deploy_module.git_changes import get_changed_files
from deploy_module.rules_reader import RulesReader
from deploy_module.deploy import DataScanManager
import os
from pathlib import Path
z = [Path(r'C:\Users\Artsem_Nikulin\PycharmProjects\DataplexAsCode\rules\datasets\DATASET_A\table_b\rules.yaml')]
x = RulesReader(z).get_datasets_with_rules()
c = DataScanManager(env='dev', datasets_with_rules=x)
c.create_data_scans()





