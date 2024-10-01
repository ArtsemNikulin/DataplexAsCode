from google.cloud import storage
from google.oauth2 import service_account
import json
import os


class CredentialsManager:
    def __init__(self, env):
      self.__cred_object_name = 'dataplex-dev-credentials.json'
      self.root_path = os.path.dirname(os.path.dirname(__file__))



    def get_credentials(self):
        with open(self.__cred_object_name)

        service_account_json = blob.download_as_string()

        service_account_info = json.loads(service_account_json)
        return service_account.Credentials.from_service_account_info(service_account_info)



