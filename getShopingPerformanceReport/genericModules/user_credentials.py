# Standard library imports
import os
import sys
import os.path
from os.path import join
import json

# Third party imports
from oauth2client.file import Storage
from oauth2client import client,tools
import httplib2
from httplib2 import Http
from googleapiclient.discovery import build
from googleads import adwords
from googleads import errors
from googleads import AdWordsClient
from googleads import oauth2
import google.ads.googleads.client


from google.ads.googleads.client import GoogleAdsClient


class Credentials:

	def __init__(self, utility:object, developer_name:str):
		"""Parameterized  constructor with argument"""

		self.utility = utility
		self.cred_file_dir = utility.get_credentials_folder_path()
		self.developer_name = developer_name

	def get_google_credentials(self, new_scopes:list=[]) -> object:

		scopes = self.get_google_api_scopes(new_scopes)
		client_secret_file = 'credentials.json'
		application_name = 'Google Sheets API Python Quickstart'
		auth_file_name = self.developer_name + '_token' + '.json'

		try:
			import argparse
			flags = argparse.ArgumentParser(
				parents=[tools.argparser]).parse_args()
		except ImportError:
			flags = None

		if not os.path.exists(self.cred_file_dir):
			os.makedirs(self.cred_file_dir)
		credential_path = os.path.join(self.cred_file_dir,auth_file_name)

		store = Storage(credential_path)
		creds = store.get()
		if not creds or creds.invalid:
			flow = client.flow_from_clientsecrets(os.path.join(
						self.cred_file_dir,client_secret_file), scopes)
			flow.user_agent = application_name

			if flags:
				creds = tools.run_flow(flow, store, flags)
			else: # Needed only for compatibility with Python 2.6
				creds = tools.run(flow, store)
			print('Storing credentials to ' + credential_path)

		return creds

	def get_merchant_center_credentials(self):

		auth_file_name = 'merchant_center_credentials.json'

		auth_file_path = os.path.join(self.cred_file_dir, auth_file_name)

		try:
			import argparse
			flags = tools.argparser.parse_args([])
		except ImportError:
			flags = None

		store = Storage(auth_file_path)
		creds = store.get()

		if not creds or creds.invalid:
			print('Error in credentials!!!\nPlease check your merchant center credentials')

		return creds

	def build_service(self, platform:str, version:str, creds:str='adwords') -> object:
		"""Builds service for any google api"""

		if creds == 'merchant_center':
			credentials = self.get_merchant_center_credentials()
		else:
			credentials = self.get_google_credentials()
		# cache_discovery=False
		service = None
		if credentials:
			service = build(platform, version, credentials=credentials)

		return service

	def get_adwords_client(self, customer_id:str) -> object:
		"""This function returns client created by you credentials"""

		customer_id = customer_id.replace('-','');
		google_yaml = self.utility.google_yaml_file()
		client = adwords.AdWordsClient.LoadFromStorage(google_yaml)
		client.SetClientCustomerId(customer_id)

		return client

	def get_google_ads_client(self):

		cred_file_dir = self.utility.get_credentials_folder_path()
		clients_secret_file = 'google-ads.yaml'
		
		google_yaml = os.path.join(cred_file_dir, clients_secret_file)
		client = google.ads.google_ads.client.GoogleAdsClient.load_from_storage(google_yaml)
		return client

	def get_google_api_scopes(self, new_scopes:list) -> list :
		"""This function appends new scopes if needed else
		returns exisiting scopes.
		"""

		scopes = [
			'https://www.googleapis.com/auth/spreadsheets',
			'https://www.googleapis.com/auth/drive',
			'https://www.googleapis.com/auth/gmail.send',
			'https://www.googleapis.com/auth/gmail.compose',
			'https://www.googleapis.com/auth/gmail.readonly',
			'https://www.googleapis.com/auth/analytics.readonly'
		]

		if new_scopes != [] :
			scopes += new_scopes

		return scopes

	def get_facebook_credentials(self) -> str:

		CLIENT_SECRET_FILE = 'facebook_credentials.json'

		facebook_cred_file = os.path.join(self.cred_file_dir,CLIENT_SECRET_FILE)
		with open(facebook_cred_file) as f:
			data = json.load(f)

			access_token = data['long_lived_access_token']
			app_id = data['app_id']
			app_secret = data['app_secret']

		return access_token,app_id,app_secret

			



	
