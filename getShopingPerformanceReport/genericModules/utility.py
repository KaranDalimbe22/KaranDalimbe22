# Standard library imports
import os
import json
import pathlib
import time
from datetime import datetime,timedelta
import pandas as pd
import json_flatten
from google.protobuf import json_format

class Utility:
	"""Class for frequently used functions"""

	def __init__(self, file, cred_path=None):
		"""Parametrised constructor.
		Args :
		file : __file__
		cred_path:
			BASE : User level folder (identified by '~')
			PARENT : Script parent folder
			CRED_PATH_KEY : Environment key whose value points custom credential folder path (e.g, 'SCRIPT_CRED_PATH','DJANGO_CRED_PATH')
		"""

		self.__parent_directory_path = self.get_base_directory_path(file)
		self.__base_directory_path = self.get_base_directory_path()
		if cred_path == 'BASE':
			self.__cred_directory_path = self.__base_directory_path
		elif cred_path == 'PARENT':
			self.__cred_directory_path = self.__parent_directory_path
		else:
			cred_path_value = os.environ.get(cred_path, '')
			if not cred_path_value:
				print(f'critical: please add "{cred_path}" to environmental variable'.upper())
				print('--------------------------')
			cred_path = cred_path_value
			self.__cred_directory_path = cred_path

	def get_base_directory_path(self, file=None) -> str:
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

	def get_current_directory_path(self) -> str:
		"""Gets path of current working directory"""

		current_directory_path = pathlib.Path().absolute()

		return current_directory_path

	def create_folder(self, folder_path:str):
		"""Creates a new folder"""

		if not os.path.exists(folder_path):
			os.makedirs(folder_path)

	def delete_file(self, file_path:str):
		"""Deletes an existing file or folder"""

		if os.path.isfile(file_path):
			os.unlink(file_path)

	def join_or_create_file_path(self, file_path:str, file_name:str):
		"""this function joins path or creates if not existed"""

		file_path = os.path.join(file_path, file_name)
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		return file_path

	def get_credentials_folder_path(self) -> str:
		"""Gets credentials folder path having all credential
		files.
		"""
		
		folder_name = 'CREDENTIALS'
		# check os.path
		cred_file_dir = os.path.join(self.cred_path, folder_name)
		print("\ncred_file_dir = ", cred_file_dir,'\n')
		return cred_file_dir

	def get_timestamp_file_path(self, SCRIPT_NAME:str) -> str:
		"""Gets path of script's timestamp file existing in Running 
		status folder or creates one if not existing.
		"""

		folder_name = 'Running status'
		folder_path_of_timestamp_files  = os.path.join(
			self.base_path, folder_name)
		if not os.path.exists(folder_path_of_timestamp_files):
			os.makedirs(folder_path_of_timestamp_files)

		file_name = SCRIPT_NAME+'_timestamp.txt'
		time_stamp_file_path = os.path.join(folder_path_of_timestamp_files,file_name)
		return time_stamp_file_path

	def get_script_log_folder_path(self) -> str:
		"""Gets path of script log folder which has temporary files
		or script's output files.
		"""

		folder_name = 'Script log'
		script_log_folder_path  = os.path.join(self.base_path, 
			folder_name)
		if not os.path.exists(script_log_folder_path):
			os.makedirs(script_log_folder_path)
	
		return script_log_folder_path

	def get_testing_files_path(self) -> str:
		"""Gets path of testing files folder which has data required
		for testing the script.
		"""

		folder_name = 'Testing files'
		testing_files_path  = os.path.join(self.base_path, 
			folder_name)
		if not os.path.exists(testing_files_path):
			os.makedirs(testing_files_path)
	
		return testing_files_path

	def google_yaml_file(self) -> str:
		"""This function gets google yaml(adwords) credentials"""

		clients_secret_file = 'googleads.yaml'
		cred_file_dir = self.get_credentials_folder_path()
		google_yaml = os.path.join(cred_file_dir,clients_secret_file)

		return google_yaml

	def get_token_file_name(self, SCRIPT_NAME:str,DEVELOPER_NAME:str) -> str:
		"""Replaces space by underscore especially in 
		token file or timestamp file.
		"""

		SCRIPT_NAME = SCRIPT_NAME.lower().replace(' ','_')
		DEVELOPER_NAME = DEVELOPER_NAME.lower().replace(' ','_')

		return SCRIPT_NAME,DEVELOPER_NAME

	def get_global_variables(self) -> dict:
		"""This functions gets global variables data from the variables 
		file.
		"""
		data = {}

		file_name = "global_variables.json"
		cred_file_dir = self.get_credentials_folder_path()
		global_variables_file = os.path.join(cred_file_dir,file_name)

		if(os.path.exists(global_variables_file)):
			with open(global_variables_file) as data_file:
				data = json.load(data_file)
		else:
			print("global_variables.json does not exists")

		return data

	def apply_thousand_separator(self, number:int) -> int:
   		"""Applies comma in number for recognizing thousands"""

   		formatted_number = f"{int(number):,}"

   		return formatted_number

	def write_script_starting_status(self, SCRIPT_NAME:str, 
		total_accounts_length:str):
	# def write_script_starting_status(self, SCRIPT_NAME:str):
		"""Write starting status of script in timestamp file"""

		time_stamp_file_path = self.get_timestamp_file_path(SCRIPT_NAME)
		current_time = time.asctime(time.localtime(time.time()))
		starting_status = "\n****************************************************************************************************\
		\nScript running started on "+str(total_accounts_length)+" accounts, start time : "+current_time
		# starting_status = "\n****************************************************************************************************\
		# \nScript running started, start time : "+current_time
		timestamp_file = open(time_stamp_file_path,"a+")
		timestamp_file.write(starting_status)
		timestamp_file.close()
	
	def write_script_running_status(self, SCRIPT_NAME:str, 
   		account_name:str, customer_id:str, status:str, changes:str="0"):
   		"""Write running status of script in timestamp file"""

   		time_stamp_file_path = self.get_timestamp_file_path(SCRIPT_NAME)
   		current_time = time.asctime(time.localtime(time.time()))
   		running_status  = "\n"+str(account_name)+ " | "+str(customer_id)+" | "+current_time+" | "+status+" | "+str(changes)
   		timestamp_file = open(time_stamp_file_path,"a+")
   		timestamp_file.write(running_status)
   		timestamp_file.close()


	def write_script_ending_status(self, SCRIPT_NAME:str, 
		total_accounts_length:str, success_count:str, fail_count:str):
		"""Write ending status of script in timestamp file"""

		time_stamp_file_path = self.get_timestamp_file_path(SCRIPT_NAME)
		current_time = time.asctime(time.localtime(time.time()))
		ending_status = '\n@@@@@ TOTAL ACCOUNTS : {} | SUCCESS : {} accounts | FAILED : {} accounts | {}'.format(total_accounts_length,success_count, fail_count, current_time)
		timestamp_file = open(time_stamp_file_path,"a+")
		timestamp_file.write(ending_status)
		timestamp_file.close()

	def get_env_config(self,production_mode:bool):
		"""Returns production or development environment variables

        Args:
            production_mode (boolean): True or False
        Return:
            Dictionary of environment variables
		"""

		mode = 'prod' if production_mode == True else 'dev'

		data = {}		# default value of data

		env_config_file_name = "env-config.json"
		env_config_file_path = os.path.join(self.parent_path, env_config_file_name)

		if(os.path.exists(env_config_file_path)):
			with open(env_config_file_path) as f:
				data = json.load(f)
		else:
			print(f"env-config.json does not exists : {env_config_file_path}")

		return data.get(mode, {})

	def get_custom_date_range(self,date_min:int, date_max:int) -> str:
		
		past_date_min = datetime.now() - timedelta(days=date_min)
		past_date_min = str(past_date_min)[:10].replace("-", "")

		past_date_max = datetime.now() - timedelta(days=date_max)
		past_date_max = str(past_date_max)[:10].replace("-", "")

		# final_date_range = past_date_min+", "+past_date_max
		
		return past_date_min, past_date_max	

	def get_BO_sheet_data(self, user_credentials:object, Spreadsheet:object, sheet_ranges:dict):
		"""This function reads BO sheet and returns dataframe."""

		spreadsheet_service = user_credentials.build_service('sheets','v4')
		global_variables = self.get_global_variables()
		BO_spreadsheet_url = global_variables['BID_OPTIMIZER_PATH_SHEET']
		bo_spreadsheet = Spreadsheet(spreadsheet_service, BO_spreadsheet_url)
		sheets_data = bo_spreadsheet.read(sheet_ranges)
		main_df = self.sheets_data_to_df(sheets_data)

		return bo_spreadsheet, main_df

	def get_adhelp_sheet_data(self, user_credentials:object, Spreadsheet:object, sheet_ranges:dict):
		"""This function reads BO sheet and returns dataframe."""

		spreadsheet_service = user_credentials.build_service('sheets','v4')
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

	def get_token_file_path(self, developer_name:str) -> str:
		"""Gets token file path for api connector.

		Args:-
		developer_name (str): Name of the script auth or developer.

		Returns:-
		token_file_path (str): Path joined by the cred folder and 
		developer name.
		"""

		developer_file_name = developer_name+'_token.json'

		cred_file_dir = self.get_credentials_folder_path()

		token_file_path = self.join_or_create_file_path(cred_file_dir, developer_file_name)

		return token_file_path

	def get_credentials_file_path(self, credential_file_name:str='credentials.json') -> str:
		"""Gets credential_file_name file path to create or use existing crdentials for api connector.

		Args:-
		credential_file_name (str): Name of the cred file.

		Returns:-
		credential_file_path (str): Path joined by the cred folder and 
		cred file name.
		"""

		cred_file_dir = self.get_credentials_folder_path()

		credential_file_path = self.join_or_create_file_path(cred_file_dir, credential_file_name)

		return credential_file_path

	def get_google_ads_yaml_path(self, google_ads_yaml_file_name:str='google-ads.yaml') -> str:
		"""Gets google_ads_yaml file path to create client.

		Args:-
		google_ads_yaml_file_name (str): Name of the yaml file.

		Returns:-
		google_ads_yaml_path (str): Path joined by the cred folder and 
		yaml name.
		"""

		cred_file_dir = self.get_credentials_folder_path()

		google_ads_yaml_path = self.join_or_create_file_path(cred_file_dir, google_ads_yaml_file_name)

		return google_ads_yaml_path
