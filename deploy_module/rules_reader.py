import yaml



class RulesReader:
    def __init__(self, pathes):
        self.base_path = pathes
        self.all_rules = []

    def get_rules(self, dataset, table):
        rules_file_path = self.base_path / dataset / table / 'rules.yaml'
        try:
            with open(rules_file_path, 'r') as file:
                rules = yaml.safe_load(file)
            return rules

        except FileNotFoundError:
            raise FileNotFoundError(f"Rules file not found: {rules_file_path}")

    def get_datasets_with_rules(self):
        base_path = self.base_path
        for dataset_path in base_path.iterdir():
            if dataset_path.is_dir():
                dataset_name = dataset_path.name
                for table_path in dataset_path.iterdir():
                    if table_path.is_dir() and (table_path / 'rules.yaml').exists():
                        rules = self.get_rules(dataset_name, table_path.name)
                        self.all_rules.append({'dataset': dataset_name,
                                               'table': table_path.name,
                                               'samplingPercent': rules.get('samplingPercent', 100),
                                               'rowFilter': rules.get('rowFilter', ""),
                                               'labels': rules.get('labels', {}),
                                               'executionSpec': rules.get('executionSpec', {}),
                                               'rules': rules.get('rules', []),
                                               })

        return self.all_rules
