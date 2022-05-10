# Standard library imports
import os
import sys
import pandas as pd

# Third party imports

# local application import
sys.path.append(os.environ['SHARED_PACKAGE_PATH'])
from genericModules.utilities import (
	OSUtils, 
	ScriptUtils
)
from genericModules.api_connectors import GoogleAPIConnectorFactory
import logic

"""
Script Schedule (https://crontab.guru/): 

Script Path (full path of file to trigger): 

Script Frequency: 

Developer Name: 

Documentation Link: --

Short description:

"""

#Global variables
PRODUCTION_MODE = False
DEVELOPER_NAME = ''
SCRIPT_NAME = 'generic script structure'

def main():

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

	env_dict = script_utility.get_env_config(PRODUCTION_MODE)
	print("Environment Details: ")
	print(env_dict)

	credentials_file_path = script_utility.get_credentials_file_path()
	auth_token_file_path = script_utility.get_token_file_path(
		DEVELOPER_NAME
	)

	api_connector_factory = GoogleAPIConnectorFactory(
		credentials_file_path, 
		auth_token_file_path
	)

	customers_df, customer_ids = logic.get_customer_ids(
		script_utility, 
		api_connector_factory
	)

	script_utility.write_script_starting_status(
		len(customer_ids)
	)

	result_count = {customer_id: False for customer_id in customer_ids}
	errors = {}

	result_count, errors = logic.main(
		PRODUCTION_MODE, 
		script_utility, 
		api_connector_factory, 
		customers_df, 
		result_count, 
		errors
	)

	success_count = sum(filter(lambda v: v is True, result_count.values()))
	fail_count = len(customer_ids) - success_count
	script_utility.write_script_ending_status(
		len(customer_ids), 
		success_count, 
		fail_count
	)


if __name__ == '__main__':
	main()






