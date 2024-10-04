from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists
from deploy_module.rules_reader import RulesReader
import yaml
import os
import uuid


class DataScanManager:
    def __init__(self, env):
        self.env = env
        self.root_path = os.path.dirname(os.path.dirname(__file__))
        self.data_quality_rule = dataplex_v1.DataQualityRule()
        self.client = dataplex_v1.DataScanServiceClient()
        if self.env.lower() == 'dev':
            self.config_file_path = os.path.join(self.root_path, 'configs/dev_config.yaml')
        elif self.env.lower() == 'prod':
            self.config_file_path = os.path.join(self.root_path, 'configs/prod_config.yaml')
        self.config = self.read_config()
        self.datasets_with_rules = RulesReader(self.config['data_quality_rules']).get_all_rules()
        self.dataplex_data_scans = self.form_data_scans()

    def read_config(self):
        with open(self.config_file_path, 'r') as config:
            config_yaml = yaml.safe_load(config)
            return config_yaml

    def create_rule(self, rule):
        self.data_quality_rule = dataplex_v1.DataQualityRule(
            column=rule.get("column", ""),
            ignore_null=rule.get('ignoreNull', False),
            dimension=rule.get("dimension", 'COMPLETENESS'),
            description=rule.get("description", ""),
            name=rule.get("name", f"rule{str(uuid.uuid4())}"),
            threshold=rule.get("threshold", 0),
            non_null_expectation=rule.get('nonNullExpectation', {}),
            range_expectation={
                'min_value': rule.get('rangeExpectation', {}).get('minValue', ""),
                'max_value': rule.get('rangeExpectation', {}).get('maxValue', ""),
                'strict_min_enabled': rule.get('rangeExpectation', {}).get('strictMinEnabled', False),
                'strict_max_enabled': rule.get('rangeExpectation', {}).get('strictMaxEnabled', False),
            } if 'rangeExpectation' in rule else {},
            regex_expectation={
                'regex': rule.get('regexExpectation', {}).get('regex', ""),
            } if 'regexExpectation' in rule else {},
            set_expectation={
                'values': rule.get('setExpectation', {}).get('values', []),
            } if 'setExpectation' in rule else {},
            uniqueness_expectation=rule.get("uniquenessExpectation", {}),
            statistic_range_expectation={
                'statistic': rule.get('statisticRangeExpectation', {}).get('statistic', ""),
                'min_value': rule.get('statisticRangeExpectation', {}).get('minValue', ""),
                'max_value': rule.get('statisticRangeExpectation', {}).get('maxValue', ""),
                'strict_min_enabled': rule.get('statisticRangeExpectation', {}).get('strictMinEnabled', False),
                'strict_max_enabled': rule.get('statisticRangeExpectation', {}).get('strictMaxEnabled', False),
            } if 'statisticRangeExpectation' in rule else {},
            row_condition_expectation={
                'sql_expression': rule.get('rowConditionExpectation', {}).get('sqlExpression', ""),
            } if 'rowConditionExpectation' in rule else {},
            table_condition_expectation={
                'sql_expression': rule.get('tableConditionExpectation', {}).get('sqlExpression', ""),
            } if 'tableConditionExpectation' in rule else {},
            sql_assertion={
                'sql_statement': rule.get('sqlAssertion', {}).get('sqlStatement', ""),
            } if 'sqlAssertion' in rule else {}
        )
        return self.data_quality_rule

    def form_data_scans(self):
        dataplex_data_scans = []
        for dataplex_scan in self.datasets_with_rules:
            dataset = dataplex_scan['dataset']
            table = dataplex_scan['table']
            dq_rules = [self.create_rule(rule) for rule in dataplex_scan['rules']]
            dataplex_data_scan = dataplex_v1.DataScan(
                {'display_name': f"{self.env}.{dataset}.{table}",
                 'data': {"resource": f"//bigquery.googleapis.com/projects/{self.config['project_id']}/datasets/"
                                      f"{dataset}/tables/{table}"},
                 'data_quality_spec': {
                     "rules": dq_rules,
                     "sampling_percent": dataplex_scan.get('samplingPercent', 100),
                     "row_filter": dataplex_scan.get('rowFilter', ""),
                     "post_scan_actions": {
                         "bigquery_export": {
                             "results_table": self.config['results_table']
                         }
                     },
                 },
                 'labels': dataplex_scan.get('labels', {}),
                 'execution_spec': {
                                    "trigger": {
                                        "schedule": {
                                            "cron": dataplex_scan.get('executionSpec', {}).get('trigger', {}).get('schedule', {}).get('cron', "")
                                        },
                                        "on_demand": dataplex_scan.get('executionSpec', {}).get('trigger', {}).get('onDemand', {})
                                    }
                                } if 'executionSpec' in dataplex_scan else {}
                 }
            )

            dataplex_data_scans.append(dataplex_data_scan)
        return dataplex_data_scans

    def create_data_scans(self, validate=False):
        data_scan_id = str(uuid.uuid4())
        for dataplex_data_scan in self.dataplex_data_scans:
            request = dataplex_v1.CreateDataScanRequest(
                parent=self.config['parent'],
                data_scan=dataplex_data_scan,
                data_scan_id=data_scan_id,
                validate_only=validate
            )

            try:
                response = self.client.create_data_scan(request=request)
                print("DataScan created successfully:", response)
            except AlreadyExists:
                print(f"DataScan '{data_scan_id} ({dataplex_data_scan.display_name})' already exists. Recreating ...")

                self.delete_data_scan(self.config['parent'], data_scan_id)

                response = self.client.create_data_scan(request=request)
                print(f"DataScan '{data_scan_id} ({dataplex_data_scan.display_name})' recreated successfully :", response)

    def delete_data_scan(self, parent, data_scan_id):
        try:
            delete_request = dataplex_v1.DeleteDataScanRequest(
                {'name': f"{parent}/dataScans/{data_scan_id}"}
            )
            self.client.delete_data_scan(request=delete_request)
            print(f"DataScan '{data_scan_id}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete DataScan '{data_scan_id}': {e}")
