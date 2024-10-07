import json

from deploy_module.deploy import DataScanManager
from deploy_module.rules_reader import RulesReader
import os
from deploy_module.git_changes import get_changed_files


x = ['asdasd/rules.yaml', 'asdasd/asdad/x.txt']
if 'rules.yaml' in x:
    print('y')
else:
    print('n')
