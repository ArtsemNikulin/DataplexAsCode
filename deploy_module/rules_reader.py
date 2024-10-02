import yaml
from pathlib import Path


class RulesReader:
    def __init__(self, base_path):
        """Initialize the RulesReader with the base path for rules."""
        self.base_path = Path(base_path)
        self.rules = {}
        self.rule_yaml = None

    def read_rules_yaml(self, file_path):
        """Load a rules YAML file and return its contents as a dictionary."""
        with open(file_path, 'r') as file:
            self.rule_yaml = yaml.safe_load(file)
            return self.rule_yaml

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
        """Retrieve all rules for all datasets and tables."""
        all_rules = {}

        for dataset_path in self.base_path.iterdir():
            if dataset_path.is_dir():
                dataset_name = dataset_path.name
                all_rules[dataset_name] = {}

                for table_path in dataset_path.iterdir():
                    if table_path.is_dir() and (table_path / 'rules.yaml').exists():
                        all_rules[dataset_name][table_path.name] = self.get_rules(dataset_name, table_path.name)

        return all_rules
