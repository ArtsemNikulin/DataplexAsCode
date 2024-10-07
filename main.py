import json

from deploy_module.deploy import DataScanManager
from deploy_module.rules_reader import RulesReader
import os
from deploy_module.git_changes import get_changed_files


x = [
    '/workspace/cloudbuild.yaml',
    '/workspace/deploy_module/deploy.py',
    '/workspace/main.py',
    '/workspace/rules/datasets/DATASET_A/table_a/rules.yaml'
]

# Filter the list for paths that end with 'rules.yaml'


