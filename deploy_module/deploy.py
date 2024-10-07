import time
from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists
import yaml
import os
import uuid
from deploy_module.git_changes import get_changed_files


class DataScanManager:
    def __init__(self, env, datasets_with_rules):
        self.env = env
        self.root_path = os.path.dirname(os.path.dirname(__file__))
        self.client = dataplex_v1.DataScanServiceClient()
        if self.env.lower() == 'dev':
            self.config_file_path = os.path.join(self.root_path, 'configs/dev_config.yaml')
        elif self.env.lower() == 'prod':
            self.config_file_path = os.path.join(self.root_path, 'configs/prod_config.yaml')
        self.config = self.read_config()
        self.datasets_with_rules = datasets_with_rules

    def read_config(self):
        with open(self.config_file_path, 'r') as config:
            config_yaml = yaml.safe_load(config)
            return config_yaml

    def form_data_scans(self):
        dataplex_data_scans = []
        for dataset_with_rules in self.datasets_with_rules:
            dataset = dataset_with_rules['dataset'].lower()
            table = dataset_with_rules['table']
            rules = dataset_with_rules['rules']
            dataplex_data_scan = dataplex_v1.DataScan()
            dataplex_data_scan.display_name = f"{self.env}.{dataset}.{table}"
            dataplex_data_scan.data.resource = f"//bigquery.googleapis.com/projects/{self.config['project_id']}/" \
                                               f"datasets/{dataset}/tables/{table}"
            dataplex_data_scan.data_quality_spec.rules = rules
            dataplex_data_scan.data_quality_spec.sampling_percent = dataset_with_rules.get('samplingPercent', 100)
            dataplex_data_scan.data_quality_spec.row_filter = dataset_with_rules.get('rowFilter', "")
            dataplex_data_scan.data_quality_spec.post_scan_actions.bigquery_export.results_table = \
                self.config['results_table']
            dataplex_data_scan.labels = dataset_with_rules.get('labels', {})
            dataplex_data_scans.append(dataplex_data_scan)

        return dataplex_data_scans

    def create_data_scans(self, validate=False):
        data_scans = self.form_data_scans()
        if len(data_scans) > 0:
            for dataplex_data_scan in data_scans:
                request = dataplex_v1.CreateDataScanRequest()
                request.parent = self.config['parent']
                request.data_scan = dataplex_data_scan
                request.data_scan_id = 'scan-' + (dataplex_data_scan.display_name.replace('.', '_')).lower()
                request.validate_only = validate

                try:
                    response = self.client.create_data_scan(request=request)
                    print(
                        f"DataScan '{request.data_scan_id} ({dataplex_data_scan.display_name})' created successfully:",
                        response)
                except AlreadyExists:
                    print(
                        f"DataScan '{request.data_scan_id} ({dataplex_data_scan.display_name})' already exists. Recreating ...")

                    self.delete_data_scan(self.config['parent'], request.data_scan_id)
                    time.sleep(5)

                    response = self.client.create_data_scan(request=request)
                    print(
                        f"DataScan '{request.data_scan_id} ({dataplex_data_scan.display_name})' recreated successfully :",
                        response)
        else:
            print("There are no formalized DataScans, probably no changes in Rules")
            print(f"Identifited changes: {get_changed_files()}")

    def delete_data_scan(self, parent, data_scan_id):
        try:
            delete_request = dataplex_v1.DeleteDataScanRequest(
                {'name': f"{parent}/dataScans/{data_scan_id}"}
            )
            self.client.delete_data_scan(request=delete_request)
            print(f"DataScan '{data_scan_id}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete DataScan '{data_scan_id}': {e}")
