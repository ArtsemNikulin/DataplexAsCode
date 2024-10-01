from deploy_module.deploy import DataScanManager

manager = DataScanManager(env='dev')  # Or 'prod' for production
manager.create_data_scan()
