import yaml
from pathlib import Path


class RulesReader:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.rules = {}
        self.rules_yaml = []

    def read_rules_yaml(self, file_path):
        with open(file_path, 'r') as file:
            data_quality_spec_raw = yaml.safe_load(file)

        rules = data_quality_spec_raw.get('rules', [])

        for rule in rules:
            parsed_rule = {
                'column': rule.get('column', None),
                'ignore_null': rule.get('ignoreNull', rule.get('ignore_null', None)),
                'dimension': rule.get('dimension', None),
                'description': rule.get('description', None),
                'name': rule.get('name', None),
                'threshold': rule.get('threshold', None),
                'non_null_expectation': rule.get('nonNullExpectation', rule.get('non_null_expectation', None)),
                'range_expectation': {
                    'min_value': rule.get('rangeExpectation', {}).get('minValue',
                                                                      rule.get('range_expectation',
                                                                               {}).get('min_value',
                                                                                       None)),
                    'max_value': rule.get('rangeExpectation', {}).get('maxValue',
                                                                      rule.get('range_expectation',
                                                                               {}).get('max_value',
                                                                                       None)),
                    'strict_min_enabled': rule.get('rangeExpectation', {}).get('strictMinEnabled',
                                                                               rule.get(
                                                                                   'range_expectation',
                                                                                   {}).get(
                                                                                   'strict_min_enabled',
                                                                                   None)),
                    'strict_max_enabled': rule.get('rangeExpectation', {}).get('strictMaxEnabled',
                                                                               rule.get(
                                                                                   'range_expectation',
                                                                                   {}).get(
                                                                                   'strict_max_enabled',
                                                                                   None)),
                } if 'rangeExpectation' in rule or 'range_expectation' in rule else None,
                'regex_expectation': {
                    'regex': rule.get('regexExpectation', {}).get('regex',
                                                                  rule.get('regex_expectation', {}).get('regex', None)),
                } if 'regexExpectation' in rule or 'regex_expectation' in rule else None,
                'set_expectation': {
                    'values': rule.get('setExpectation', {}).get('values',
                                                                 rule.get('set_expectation', {}).get('values', None)),
                } if 'setExpectation' in rule or 'set_expectation' in rule else None,
                'uniqueness_expectation': rule.get('uniquenessExpectation', rule.get('uniqueness_expectation', None)),
                'statistic_range_expectation': {
                    'statistic': rule.get('statisticRangeExpectation', {}).get('statistic',
                                                                               rule.get('statistic_range_expectation',
                                                                                        {}).get('statistic', None)),
                    'min_value': rule.get('statisticRangeExpectation', {}).get('minValue',
                                                                               rule.get('statistic_range_expectation',
                                                                                        {}).get('min_value', None)),
                    'max_value': rule.get('statisticRangeExpectation', {}).get('maxValue',
                                                                               rule.get('statistic_range_expectation',
                                                                                        {}).get('max_value', None)),
                    'strict_min_enabled': rule.get('statisticRangeExpectation', {}).get('strictMinEnabled', rule.get(
                        'statistic_range_expectation', {}).get('strict_min_enabled', None)),
                    'strict_max_enabled': rule.get('statisticRangeExpectation', {}).get('strictMaxEnabled', rule.get(
                        'statistic_range_expectation', {}).get('strict_max_enabled', None)),
                } if 'statisticRangeExpectation' in rule or 'statistic_range_expectation' in rule else None,
                'row_condition_expectation': {
                    'sql_expression': rule.get('rowConditionExpectation', {}).get('sqlExpression',
                                                                                  rule.get('row_condition_expectation',
                                                                                           {}).get('sql_expression',
                                                                                                   None)),
                } if 'rowConditionExpectation' in rule or 'row_condition_expectation' in rule else None,
                'table_condition_expectation': {
                    'sql_expression': rule.get('tableConditionExpectation', {}).get('sqlExpression',
                                                                                    rule.get(
                                                                                        'table_condition_expectation',
                                                                                        {}).get('sql_expression',
                                                                                                None)),
                } if 'tableConditionExpectation' in rule or 'table_condition_expectation' in rule else None,
                'sql_assertion': {
                    'sql_expression': rule.get('sqlAssertion', {}).get('sqlStatement',
                                                                       rule.get('sql_assertion',
                                                                                {}).get('sql_expression',
                                                                                        None)),
                } if 'sqlAssertion' in rule or 'sql_assertion' in rule else None,
            }
            self.rules_yaml.append(parsed_rule)

        return self.rules_yaml

    def get_rules(self, dataset, table):
        """Retrieve rules for a specific dataset and table."""
        rule = (dataset, table)

        if rule in self.rules:
            return self.rules[rule]

        rules_file_path = self.base_path / dataset / table / 'rules.yaml'

        try:
            rules = self.read_rules_yaml(rules_file_path)
            self.rules[rule] = rules
            return rules
        except FileNotFoundError:
            raise FileNotFoundError(f"Rules file not found: {rules_file_path}")

    def get_all_rules(self):
        all_rules = {}

        for dataset_path in self.base_path.iterdir():
            if dataset_path.is_dir():
                dataset_name = dataset_path.name
                all_rules[dataset_name] = {}

                for table_path in dataset_path.iterdir():
                    if table_path.is_dir() and (table_path / 'rules.yaml').exists():
                        all_rules[dataset_name][table_path.name] = self.get_rules(dataset_name, table_path.name)

        return all_rules
