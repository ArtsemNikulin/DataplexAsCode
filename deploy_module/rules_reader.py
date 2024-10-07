import yaml


class RulesReader:
    def __init__(self, pathes):
        self.base_path = pathes
        self.all_rules = []

    def get_rules(self, rules_file_path):
        try:
            with open(rules_file_path, 'r') as file:
                rules = yaml.safe_load(file)
            return rules

        except FileNotFoundError:
            raise FileNotFoundError(f"Rules file not found: {rules_file_path}")

    def get_datasets_with_rules(self):
        for rules_path in self.base_path:
            if rules_path.exists():
                dataset_name = rules_path.parts[-3]
                table_name = rules_path.parts[-2]
                rules = self.get_rules(rules_path)
                self.all_rules.append({'dataset': dataset_name,
                                       'table': table_name,
                                       'samplingPercent': rules.get('samplingPercent', 100),
                                       'rowFilter': rules.get('rowFilter', ""),
                                       'labels': rules.get('labels', {}),
                                       'executionSpec': rules.get('executionSpec', {}),
                                       'rules': rules.get('rules', []),
                                       })
            else:
                print(f"Rules were deleted, since path does not exist - {rules_path}")
                print("Corresponding data scan will be deleted")
                self.all_rules.append(rules_path)

        return self.all_rules
