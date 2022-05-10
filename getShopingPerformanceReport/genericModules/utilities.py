# Standard library imports
import os
import sys
import time
from datetime import (
	datetime,
	timedelta
)
import json

# Third party imports
import pandas as pd

# Local application imports


class OSUtils:
	"""Class for frequently used functions"""

	def __init__(self, file, directory_type='BASE', env_key=None):
		"""Parametrised constructor.
		Args :
		file : __file__
		directory_type:
			BASE : User level folder (identified by '~') for cred path
			PARENT : Script parent folder for cred path
			ENV : Use env_key for cred path
		env_key: Environment key whose value points custom credential folder path (e.g, 'SCRIPT_CRED_PATH','DJANGO_CRED_PATH')
		"""

		self.__parent_directory_path = self._get_directory_path(file)
		self.__base_directory_path = self._get_directory_path()

		cred_path = ''
		if directory_type == 'BASE':
			cred_path = self.__base_directory_path

		elif directory_type == 'PARENT':
			cred_path = self.__parent_directory_path

		elif directory_type == 'ENV':
			cred_path = os.environ.get(env_key, '')
			if not cred_path:
				print(f'critical: please add "{cred_path}" to environmental variable'.upper())
				print('--------------------------')

		print('\nScript Credentials Path = ', cred_path)

		self.__cred_directory_path = cred_path

	def _get_directory_path(self, file=None) -> str:
		"""Gets path for base directory of system"""

		# If file is None then takes from system, # else takes folder
		if file is None:
			base_directory = os.path.expanduser('~')
		else:
			parent_dir = os.path.dirname(os.path.abspath(file))
			base_directory = parent_dir

		return base_directory

	@property
	def base_path(self) -> str:
		"""Getter for base directory path"""

		return self.__base_directory_path

	@property
	def parent_path(self) -> str:
		"""Getter for parent directory path"""

		return self.__parent_directory_path

	@property
	def cred_path(self) -> str:
		"""Getter for cred directory path"""

		return self.__cred_directory_path

	def create_folder(self, *paths:tuple):
		"""this function joins path or creates if not existed"""

		folder_path = self._create(*paths)

		return folder_path

	def delete_folder(self, *paths:str):
		"""Deletes an existing folder"""

		self._delete(*paths)

	def create_file(self, *paths:tuple):
		"""this function joins path or creates if not existed"""

		file_path = self._create(*paths)

		return file_path

	def delete_file(self, *paths:str):
		"""Deletes an existing file"""

		self._delete(*paths)

	def _create(self, *paths):

		path = self.join(*paths)
		exists = self.is_exists(path)
		if not exists:
			os.makedirs(path)

		return path

	def _delete(self, *paths):

		path = self.join(*paths)
		exists = self.is_exists(path)
		if exists:
			os.unlink(path)
		else:
			print(f'File does not exists at path: {path}')

	def join(self, *paths:tuple):
		"""this function joins path or creates if not existed"""

		path = os.path.join(*paths)

		return path

	def is_exists(self, path:str):
		exists = False
		if path:
			exists = os.path.exists(path)

		return exists


class ScriptUtils:

	def __init__(self, os_utils, script_name):
		self.os_utils = os_utils
		self.script_name = script_name.replace(' ','_').lower().strip()

	def get_credentials_folder_path(self) -> str:
		"""Gets credentials folder path having all credential
		files.
		"""

		os_utils = self.os_utils

		folder_name = 'CREDENTIALS'

		cred_file_dir = os_utils.join(os_utils.cred_path, folder_name)

		if not os_utils.is_exists(cred_file_dir):
			print(f'folder does not exists at path: {cred_file_dir}')

		return cred_file_dir

	def get_credentials_file_path(self, credential_file_name:str='credentials.json') -> str:
		"""Gets credential_file_name file path to create or use existing crdentials for api connector.

		Args:-
		credential_file_name (str): Name of the cred file.

		Returns:-
		credential_file_path (str): Path joined by the cred folder and 
		cred file name.
		"""

		os_utils = self.os_utils

		cred_file_dir = self.get_credentials_folder_path()

		credential_file_path = os_utils.join(cred_file_dir, credential_file_name)

		if not os_utils.is_exists(credential_file_path):
			print(f'file does not exists at path: {credential_file_path}')

		return credential_file_path

	def get_google_ads_yaml_path(self, google_ads_yaml_file_name:str='google-ads.yaml') -> str:
		"""Gets google_ads_yaml file path to create client.

		Args:-
		google_ads_yaml_file_name (str): Name of the yaml file.

		Returns:-
		google_ads_yaml_path (str): Path joined by the cred folder and 
		yaml name.
		"""

		os_utils = self.os_utils

		cred_file_dir = self.get_credentials_folder_path()

		google_ads_yaml_path = os_utils.join(cred_file_dir, google_ads_yaml_file_name)

		if not os_utils.is_exists(google_ads_yaml_path):
			print(f'file does not exists at path: {google_ads_yaml_path}')

		return google_ads_yaml_path

	def get_token_file_path(self, token_file_name:str) -> str:
		"""Gets token file path for api connector.

		Args:-
		token_file_name (str): Name of the script auth or developer.

		Returns:-
		token_file_path (str): Path joined by the cred folder and 
		developer name.
		"""

		if '_token.json' not in token_file_name:
			token_file_name = token_file_name.replace(' ','_').replace('@','_').replace('.','_').lower().strip()
			token_file_name = token_file_name+'_token.json'

		cred_file_dir = self.get_credentials_folder_path()

		token_file_path = self.os_utils.join(cred_file_dir, token_file_name)

		if not self.os_utils.is_exists(token_file_path):
			print(f'file does not exists at path: {token_file_path}')

		return token_file_path

	def get_script_log_folder_path(self) -> str:
		"""Gets path of script log folder which has temporary files
		or script's output files.
		"""

		os_utils = self.os_utils

		folder_name = 'scriptLog'
		script_log_folder_path = os_utils.create_folder(os_utils.base_path, 
			folder_name, self.script_name)

		return script_log_folder_path

	def get_testing_files_path(self) -> str:
		"""Gets path of testing files folder which has data required
		for testing the script.
		"""

		os_utils = self.os_utils

		folder_name = 'testLog'
		testing_files_path = os_utils.create_folder(os_utils.base_path, folder_name, self.script_name)

		return testing_files_path

	def write_script_starting_status(self, total_accounts_length:str):
		"""Write starting status of script in timestamp file"""

		current_time = time.asctime(time.localtime(time.time()))
		starting_status = "\n****************************************************************************************************\
		\nScript running started on "+str(total_accounts_length)+" accounts, start time : "+current_time
		self.write_status_in_file(starting_status)

	def write_script_running_status(self, account_name:str, customer_id:str, status:str, changes:str="0"):
		"""Write running status of script in timestamp file"""

		current_time = time.asctime(time.localtime(time.time()))
		running_status  = "\n"+str(account_name)+ " | "+str(customer_id)+" | "+current_time+" | "+status+" | "+str(changes)
		self.write_status_in_file(running_status)

	def write_script_ending_status(self, total_accounts_length:str, success_count:str, fail_count:str):
		"""Write ending status of script in timestamp file"""

		current_time = time.asctime(time.localtime(time.time()))
		ending_status = '\n@@@@@ TOTAL ACCOUNTS : {} | SUCCESS : {} accounts | FAILED : {} accounts | {}'.format(total_accounts_length,success_count, fail_count, current_time)
		self.write_status_in_file(ending_status)

	def write_status_in_file(self, status_to_write:str):

		time_stamp_file_path = self.get_timestamp_file_path()
		timestamp_file = open(time_stamp_file_path,"a+")
		timestamp_file.write(status_to_write)
		timestamp_file.close()

	def get_timestamp_file_path(self) -> str:
		"""Gets path of script's timestamp file existing in Running 
		status folder or creates one if not existing.
		"""

		script_name = self.script_name
		os_utils = self.os_utils

		folder_name = 'Running status'
		folder_path_of_timestamp_files  = os_utils.create_folder(os_utils.base_path, folder_name)

		script_file_name = script_name+'_timestamp.txt'
		time_stamp_file_path =os_utils.join(folder_path_of_timestamp_files,script_file_name)

		return time_stamp_file_path

	def get_env_config(self, production_mode:bool):
		"""Returns production or development environment variables

		Args:
			production_mode (boolean): True or False
		Return:
			Dictionary of environment variables
		"""

		os_utils = self.os_utils

		mode = 'prod' if production_mode == True else 'dev'

		data = {}		# default value of data

		env_config_file_name = "env-config.json"
		env_config_file_path = os_utils.join(os_utils.parent_path, env_config_file_name)

		data = JSONUtils.read(self, os_utils, env_config_file_path)

		base_env_data = {}

		if data:
			base_env_data = data.get('base', {})
			mode_env_data = data.get(mode, {})

			base_env_data.update(mode_env_data)

		return base_env_data

	def get_global_variables(self) -> dict:
		"""This functions gets global variables data from the variables 
		file.
		"""

		os_utils = self.os_utils

		data = {}

		file_name = "global_variables.json"
		cred_file_dir = self.get_credentials_folder_path()
		global_variables_file_path = os_utils.join(cred_file_dir, file_name)

		data = JSONUtils.read(self, os_utils, global_variables_file_path)

		return data

	def apply_thousand_separator(self, number:int) -> int:
		"""Applies comma in number for recognizing thousands"""

		formatted_number = f"{number:,}"

		return formatted_number

	def get_custom_date_range(self, date_min:int, date_max:int) -> str:

		# modify for custom date ranges also not just w.r.t today

		start_date = datetime.now() - timedelta(days=date_min)
		start_date = start_date.strftime("%Y-%m-%d")

		end_date = datetime.now() - timedelta(days=date_max)
		end_date = end_date.strftime("%Y-%m-%d")

		return start_date, end_date

	def get_BO_sheet_data(self, spreadsheet_service:object, Spreadsheet:object, sheet_ranges:dict):
		"""This function reads BO sheet and returns dataframe."""

		global_variables = self.get_global_variables()
		BO_spreadsheet_url = global_variables['BID_OPTIMIZER_PATH_SHEET']

		bo_spreadsheet = Spreadsheet(spreadsheet_service, BO_spreadsheet_url)
		sheets_data = bo_spreadsheet.read(sheet_ranges)

		main_df = self.sheets_data_to_df(sheets_data)

		return bo_spreadsheet, main_df

	def get_adhelp_sheet_data(self, spreadsheet_service:object, Spreadsheet:object, sheet_ranges:dict):
		"""This function reads BO sheet and returns dataframe."""

		global_variables = self.get_global_variables()
		adhelp_spreadsheet_url = global_variables['ADHELP_SHEET']

		adhelp_spreadsheet = Spreadsheet(spreadsheet_service, adhelp_spreadsheet_url)
		sheets_data = adhelp_spreadsheet.read(sheet_ranges)

		main_df = self.sheets_data_to_df(sheets_data)

		return adhelp_spreadsheet, main_df

	def sheets_data_to_df(self, sheets_data:dict):
		main_df = pd.DataFrame()
		for sheet_name, range_values in sheets_data.items():
			for sheet_range, sheet_values in range_values.items():
				df = pd.DataFrame.from_dict(sheet_values)
				if main_df.empty:
					main_df = df
				else:
					main_df = pd.concat([main_df,df], axis=1)

		main_df.columns = main_df.iloc[0].tolist()
		main_df.drop([0],inplace = True)
		main_df.reset_index(drop=True,inplace = True)
		main_df = main_df.fillna('--')

		return main_df

	def facebook_insights_to_df(self, insights:list):

		df = pd.DataFrame()
		count = 0
		for insight_dict in insights:
			for key,dict_value in insight_dict.items():
				if isinstance(dict_value, list):
					for list_dict in dict_value:
						if isinstance(list_dict, dict):
							if 'action_type' in list_dict.keys():
								df.loc[count,key.title()+' | '+list_dict['action_type'].title()] = list_dict['value']
							else:
								if 'value' in list_dict.keys():
									df.loc[count,key.title()] = list_dict['value']
								else:
									for key3, value3 in list_dict.items():
										df.loc[count,key.title()+' | '+key3.title()] = str(value3)
						else:
							df.loc[count,key.title()] = list_dict
				elif isinstance(dict_value, str):
					df.loc[count,key.title()] = dict_value
				elif (isinstance(dict_value, int) or isinstance(dict_value, float) or isinstance(dict_value, bool)):
					df.loc[count,key.title()] = dict_value
				else:
					dict_value = dict(dict_value)
					for key1,value1 in dict_value.items():
						df.loc[count,key.title()+' | '+key1.title()] = value1
			count += 1

		if not df.empty:
			df = df.fillna('0')

		print(df)

		return df

	def analytics_report_to_df(self, response:list):

		list_data = []
		# get report data
		for report in response.get('reports', []):
			# set column headers
			columnHeader = report.get('columnHeader', {})
			dimensionHeaders = columnHeader.get('dimensions', [])
			metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
			rows = report.get('data', {}).get('rows', [])

		for row in rows:
			# create dict for each row
			dict_row = {}
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])

			# fill dict with dimension header (key) and dimension value (value)
			for header, dimension in zip(dimensionHeaders, dimensions):
				dict_row[header] = dimension

			# fill dict with metric header (key) and metric value (value)
			for i, values in enumerate(dateRangeValues):
				for metric, value in zip(metricHeaders, values.get('values')):
				#set int as int, float a float
					if ',' in value or '.' in value:
						dict_row[metric.get('name')] = float(value)
					else:
						dict_row[metric.get('name')] = int(value)

			list_data.append(dict_row)

		df = pd.DataFrame(list_data)
		print(df)

		return df


class JSONUtils:

	@staticmethod
	def read(self, os_utils, file_path):
		data = {}
		if not os_utils.is_exists(file_path):
			print(f'json file does not exists at path: {file_path}')
			return data

		with open(file_path, 'r') as file:
			data = json.load(file)

		return data

	@staticmethod
	def write(self, file_path, data):
		with open(file_path, 'w') as file:
			json.dump(data, file)
