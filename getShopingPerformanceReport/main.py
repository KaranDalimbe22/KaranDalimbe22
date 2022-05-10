# Standard library imports
import os
import pandas as pd

# Third party imports

# local application import
from genericModules.utilities import (
	OSUtils,
	ScriptUtils
)
from genericModules.api_connectors import GoogleAPIConnectorFactory
from genericModules.api_connectors import GoogleAdsAPIConnector
import logic


"""
Script Schedule (https://crontab.guru/): 

Script Path (full path of file to trigger): 

Script Frequency: 

Developer Name: 

Documentation Link: --

Short description:

"""

# Global variables
PRODUCTION_MODE = True
SCRIPT_NAME = 'Testing'
GOOGLE_ADS_API_VERSION = 'v10'


def main(customer_id,
		account_name,
		url
		):

	os_utility = OSUtils(
		__file__,
		directory_type='BASE'
	)

	script_utility = ScriptUtils(
		os_utility,
		SCRIPT_NAME
	)

	print("--------------------------")
	print("PRODUCTION MODE : {}".format(PRODUCTION_MODE))
	print("--------------------------")

	env_config = script_utility.get_env_config(PRODUCTION_MODE)
	print("Environment Details: ")
	print(env_config)

	developer_email = env_config.get('developer_email')

	credentials_file_path = script_utility.get_credentials_file_path()
	
	auth_token_file_path = script_utility.get_token_file_path(
		developer_email
	)
	
	merchant_center_creds_file_path = script_utility.get_credentials_file_path('merchant_center_credentials.json')

	google_ads_yaml_path = script_utility.get_google_ads_yaml_path()

	google_api_connector_factory = GoogleAPIConnectorFactory(
		credentials_file_path,
		auth_token_file_path
	)

	merchant_center_api_connector_factory = GoogleAPIConnectorFactory(
		credentials_file_path,
		merchant_center_creds_file_path
	)

	google_ads_api_connector= GoogleAdsAPIConnector(
		google_ads_yaml_path,
		GOOGLE_ADS_API_VERSION
	)
	
	
	customers_df= pd.DataFrame([[customer_id,account_name]],columns=['Customer ID','Account Name'])
	customer_ids=[customer_id]

	script_utility.write_script_starting_status(
		len(customer_ids)
	)

	result_count = {customer_id: False for customer_id in customer_ids}
	errors = {}

	logic_utlity = logic.Logic(
		PRODUCTION_MODE,
		script_utility,
		customers_df,
		google_api_connector_factory,
		google_ads_api_connector,
		merchant_center_api_connector_factory,
		result_count,
		errors,
		env_config,
		url,
	)

	result_count, errors = logic_utlity.main()

	success_count = sum(filter(lambda v: v is True, result_count.values()))
	fail_count = len(customer_ids) - success_count
	script_utility.write_script_ending_status(
		len(customer_ids),
		success_count,
		fail_count
	)

	
if __name__ == '__main__':
	
	customer_id = input("Enter Customer Id")
	account_name = input("Enter Account Name")
	url = input("Enter the Spreadsheet URL")
	main(customer_id,account_name,url)