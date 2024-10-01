from deploy_module.credentials import CredentialsManager

manager = CredentialsManager('dev')
credentials = manager.get_credentials()
print(credentials)