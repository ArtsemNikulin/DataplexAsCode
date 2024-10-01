from google.cloud import storage
from google.oauth2 import service_account
import json


class CredentialsManager:
    def __init__(self, env):
        self.__bucket_name = 'dataplex_creds'
        if env.lower() == 'dev':
            self.__cred_object_name = 'dataplex-dev-credentials.json'
        else:
            self.__cred_object_name = 'dataplex-prod-credentials.json'

        self.storage_client = storage.Client()

    def get_credentials(self):
        bucket = self.storage_client.bucket(self.__bucket_name)
        blob = bucket.blob(self.__cred_object_name)

        service_account_json = blob.download_as_string()

        service_account_info = json.loads(service_account_json)
        return service_account.Credentials.from_service_account_info(service_account_info)



