import json
from deploy_module.rules_reader import RulesReader

x = RulesReader(r"C:\Users\Artsem_Nikulin\Downloads\orders.yaml").read_rules_yaml(r"C:\Users\Artsem_Nikulin\PycharmProjects\DataplexAsCode\rules\dev\datasets\DATASET_A\table_a\rules.yaml")
print(
    json.dumps(x, indent=4)
)

