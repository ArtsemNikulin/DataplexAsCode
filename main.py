from deploy_module.rules_reader import RulesReader



rules = RulesReader('rules/dev/datasets').get_all_rules()
print(rules)