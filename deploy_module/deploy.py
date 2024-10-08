from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists, NotFound, InvalidArgument
import yaml
import os
from google.protobuf import field_mask_pb2


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
        self.update_mask = field_mask_pb2.FieldMask(paths=[
            "display_name",
            "execution_spec.trigger.schedule",
            "data_quality_spec.rules",
            "data_quality_spec.sampling_percent",
            "data_quality_spec.post_scan_actions"
        ])

    def read_config(self):
        with open(self.config_file_path, 'r') as config:
            config_yaml = yaml.safe_load(config)
            return config_yaml

    def form_data_scans(self):
        dataplex_data_scans = []
        for dataset_with_rules in self.datasets_with_rules:
            if type(dataset_with_rules) == dict:
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
                cron = dataset_with_rules.get('executionSpec', {}).get('trigger', {}).get('schedule', {}).get('cron',
                                                                                                              None)

                if cron is not None:
                    dataplex_data_scan.execution_spec.trigger.schedule.cron = cron
                else:
                    dataplex_data_scan.execution_spec.trigger.on_demand = {}

                dataplex_data_scans.append(dataplex_data_scan)

            else:
                dataplex_data_scans.append(dataset_with_rules)

        return dataplex_data_scans

    def create_data_scans(self, validate=False):
        data_scans = self.form_data_scans()
        for dataplex_data_scan in data_scans:
            print(type(dataplex_data_scan))
            if not isinstance(dataplex_data_scan, dataplex_v1.types.datascans.DataScan):
                print(f"The following rules were deleted: {dataplex_data_scan}")
                dataset_name = dataplex_data_scan.parts[-3]
                table_name = dataplex_data_scan.parts[-2]
                data_scan_id = (
                    f"scan-{self.env}-{dataset_name}-{table_name}".translate(str.maketrans('._', '--'))
                ).lower()
                print(data_scan_id)
                if validate:
                    print("Delete would performed within Deploy")
                else:
                    print('Deleting scans ...')
                    self.delete_data_scan(parent=self.config['parent'],
                                          data_scan_id=data_scan_id)

            else:
                request = dataplex_v1.CreateDataScanRequest()
                request.parent = self.config['parent']
                request.data_scan = dataplex_data_scan
                request.data_scan_id = 'scan-' + (
                    dataplex_data_scan.display_name.translate(str.maketrans('._', '--'))).lower()
                request.validate_only = validate

                try:
                    response = self.client.create_data_scan(request=request)
                    print(f"DataScan '{request.data_scan_id} ({dataplex_data_scan.display_name})'"
                          f" created successfully:", response)
                except AlreadyExists:
                    print(f"DataScan '{request.data_scan_id} ({dataplex_data_scan.display_name})' "
                          f"already exists. Updating ...")

                    self.update_data_scan(data_scan=request.data_scan,
                                          data_scan_id=request.data_scan_id,
                                          validate=validate)

    def update_data_scan(self, data_scan, data_scan_id, validate):
        data_scan.name = f"{self.config['parent']}/dataScans/{data_scan_id}"

        update_request = dataplex_v1.UpdateDataScanRequest()
        update_request.data_scan = data_scan
        update_request.update_mask = self.update_mask
        update_request.validate_only = validate

        try:
            response = self.client.update_data_scan(request=update_request)
            print("Updated DataScan:", response)
        except InvalidArgument as e:
            print("Error updating DataScan:", e)
        except Exception as e:
            raise RuntimeError(f"An error occurred while updating DataScan '{data_scan.display_name}': {e}")

    def delete_data_scan(self, parent, data_scan_id):
        try:
            delete_request = dataplex_v1.DeleteDataScanRequest(
                {'name': f"{parent}/dataScans/{data_scan_id}"}
            )
            self.client.delete_data_scan(request=delete_request)
            print(f"DataScan '{data_scan_id}' deleted successfully.")
        except NotFound as e:
            print(f"DataScan '{data_scan_id}' already deleted: {e}")
        except Exception as e:
            raise RuntimeError(f"An error occurred while deleting DataScan '{data_scan_id}': {e}")
