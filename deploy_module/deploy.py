from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists
import yaml
import os


class DataScanManager:
    def __init__(self, env):
        self.env = env
        self.root_path = os.path.dirname(os.path.dirname(__file__))
        self.config = self.read_config()
        self.client = dataplex_v1.DataScanServiceClient()

    def read_config(self):
        """Load a config YAML file and return its contents as a dictionary."""
        if self.env.lower() == 'dev':
            config_file_path = os.path.join(self.root_path, 'configs/dev_config.yaml')
        elif self.env.lower() == 'prod':
            config_file_path = os.path.join(self.root_path, 'configs/prod_config.yaml')
        print(f'!!!!!!!!!!!!!!!!!!!{config_file_path}!!!!!!!!!!!!!!!!!!!!!!!!')

        with open(config_file_path, 'r') as config:
            config_yaml = yaml.safe_load(config)
            return config_yaml

    def create_data_scan(self, validate=False):
        data_scan_id = "py-scan"

        data_scan = dataplex_v1.DataScan()

        # Create a DataQualityRule object
        rule = dataplex_v1.DataQualityRule(
            column="id",
            dimension="COMPLETENESS",
            description="test",
            non_null_expectation={},
            threshold=1.0,
            name="testsets"
        )

        # Add the rule to the data_quality_spec
        data_scan.data_quality_spec.rules.append(rule)

        # Set the entity to point to the BigQuery table
        data_scan.data.resource = f"//bigquery.googleapis.com/projects/{self.config['project_id']}/datasets/dataset_a/tables/table_a"

        # Create the request
        request = dataplex_v1.CreateDataScanRequest(
            parent=self.config['parent'],
            data_scan=data_scan,
            data_scan_id=data_scan_id,
            validate_only=validate
        )

        try:
            response = self.client.create_data_scan(request=request)
            print("DataScan created successfully:", response)
        except AlreadyExists:
            print(f"DataScan '{data_scan_id}' already exists. Recreating ...")
            self.delete_data_scan(self.config['parent'], data_scan_id)
            request = dataplex_v1.CreateDataScanRequest(
                parent=self.config['parent'],
                data_scan=data_scan,
                data_scan_id=data_scan_id,
                validate_only=validate
            )
            response = self.client.create_data_scan(request=request)
            print("DataScan recreated successfully :", response)

    def delete_data_scan(self, parent, data_scan_id):
        """Delete the existing DataScan if it exists."""
        try:
            delete_request = dataplex_v1.DeleteDataScanRequest(
                {'name': f"{parent}/dataScans/{data_scan_id}"}
            )
            self.client.delete_data_scan(request=delete_request)
            print(f"DataScan '{data_scan_id}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete DataScan '{data_scan_id}': {e}")


