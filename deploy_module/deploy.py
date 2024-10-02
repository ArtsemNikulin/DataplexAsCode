from google.auth import default  # Ensure you have google-auth installed
from google.auth.transport.requests import Request
from google.cloud import dataplex_v1
import json
import os
import yaml
import requests


class DataScanManager:
    def __init__(self, env='dev'):
        self.env = env
        self.root_path = os.path.dirname(os.path.dirname(__file__))
        self.config = self.read_config()
        self.project_id = self.config['project_id']
        self.location = 'us-central1'
        self.client = dataplex_v1.DataScanServiceClient()

    def read_config(self):
        """Load a config YAML file and return its contents as a dictionary."""
        if self.env.lower() == 'dev':
            config_file_path = os.path.join(self.root_path, 'configs/dev_config.yaml')
        else:
            config_file_path = os.path.join(self.root_path, 'configs/prod_config.yaml')

        with open(config_file_path, 'r') as config:
            config_yaml = yaml.safe_load(config)
            return config_yaml

    def validate_data_scan(self, data_scan_id):
        """Validate if a DataScan can be created without actually creating it."""
        url = f"https://dataplex.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/dataScans?dataScanId={data_scan_id}&validateOnly=true"

        # Prepare the request body
        request_body = {
            "data": {
                "resource": f"//bigquery.googleapis.com/projects/{self.project_id}/datasets/dataset_a/tables/table_a"
            },
            "dataQualitySpec": {
                "rules": [
                    {
                        "column": "id",
                        "dimension": "COMPLETENESS",
                        "description": "test",
                        "nonNullExpectation": {},
                        "threshold": 1.0,
                        "name": "testsets"
                    }
                ]
            }
        }

        # Get the default credentials
        credentials, _ = default()
        credentials.refresh(Request())  # Refresh the credentials to get a valid access token
        access_token = credentials.token  # Get the access token

        # Set the headers
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        # Make the POST request
        response = requests.post(url, headers=headers, json=request_body)

        # Handle the response
        if response.status_code == 200:
            print("Validation successful: The DataScan can be created.")
            return True
        else:
            print(f"Validation failed: {response.status_code} - {response.text}")
            return False
