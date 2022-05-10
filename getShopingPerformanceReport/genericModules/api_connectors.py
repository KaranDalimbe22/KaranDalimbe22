# Standard library imports
from abc import ABC
import json

# Third party imports
from oauth2client.file import Storage
from oauth2client import client, tools
from googleapiclient.discovery import build
from google.ads.googleads.client import GoogleAdsClient
from facebook_business import FacebookSession
from facebook_business import FacebookAdsApi

# Local application imports


class GoogleAPIConnector(ABC):
	"""Base Class for google credentials"""

	_api_name = 'Google'
	_api_version = 'v0'

	_default_scopes = [
		'https://www.googleapis.com/auth/spreadsheets',
		'https://www.googleapis.com/auth/drive.file',
		'https://www.googleapis.com/auth/drive.readonly',
		'https://www.googleapis.com/auth/gmail.send',
		'https://www.googleapis.com/auth/gmail.compose',
		'https://www.googleapis.com/auth/gmail.readonly',
		'https://www.googleapis.com/auth/analytics.readonly'
	]

	def __init__(self, credentials_file_path:str, auth_token_file_path:str, api_version:str=None):
		"""Parameterised constructor of the base class.

		Args:-
			credentials_file_path (str): credentials.json or client_secrets.
			json file path.
			auth_token_file_path (str): Token JSON file path.

		Examples:-
			auth_token_file_path (str): {developer_name}_token.json
		"""

		self.credentials_file_path = credentials_file_path
		self.auth_token_file_path = auth_token_file_path
		self._api_version = api_version

		if not self._api_version:
			self._api_version = self._default_api_version

		print(f'\napi_name: {self._api_name}, api_version: {self._api_version}\n')


	def get_default_scopes(self) -> list:
		"""Getter for _default_scopes variable.

		Returns:-
			_default_scopes (list): declared defined scopes list

		"""

		return self._default_scopes

	def _create_credentials(self, store:object) -> object:
		"""Create credentials if invalid or new.

		Returns:-
			store (object): Browser url flow to create credentials.
		"""

		application_name = 'Google API Python'
		try:
			import argparse
			flags = argparse.ArgumentParser(
				parents=[tools.argparser]).parse_args()
		except ImportError:
			flags = None

		flow = client.flow_from_clientsecrets(self.credentials_file_path, self._default_scopes)
		flow.user_agent = application_name

		if flags:
			creds = tools.run_flow(flow, store, flags)
		else: # Needed only for compatibility with Python 2.6
			creds = tools.run(flow, store)
		print('Storing credentials to ' + self.auth_token_file_path)

		return creds

	def _build_service(self, api_name:str, api_version:str, discovery_uri:str=None) -> object:
		"""Builds service for any kind of google api.

		Args:-
			api_name (str): Name of the api to be used.
			api_version (str): Version of the api.
			discovery_uri (str) : Default is None, Uri for the the api to
			connect publiched discovery service on google cloud
			platform.

		Returns:-
			service (object): service/connector to initiate api calls.
		"""

		store = Storage(self.auth_token_file_path)
		creds = store.get()
		if not creds or creds.invalid:
			creds = self._create_credentials(store)

		# discovery_uri is optional
		optional_parameters = {'cache_discovery':False}
		if discovery_uri:
			optional_parameters['discoveryServiceUrl'] = discovery_uri

		service = build(api_name, api_version, credentials=creds, **optional_parameters)

		return service

	def connect(self) -> object:
		"""Connector to build service for the api calls.

		Returns:-
			service (object): service/connector to initiate api calls.
		"""

		service = self._build_service(self._api_name, self._api_version)

		return service

	def __str__(self):

		return f"{self._api_name} {self._api_version}"

class SpreadsheetConnector(GoogleAPIConnector):
	"""SpreadsheetConnector Derived Class extending GoogleAPIConnector
	Base Class."""

	_api_name = 'sheets'
	_default_api_version = 'v4'

class GmailConnector(GoogleAPIConnector):
	"""GmailConnector Derived Class extending GoogleAPIConnector
	Base Class."""

	_api_name = 'gmail'
	_default_api_version = 'v1'

class DriveConnector(GoogleAPIConnector):
	"""DriveConnector Derived Class extending GoogleAPIConnector
	Base Class."""

	_api_name = 'drive'
	_default_api_version = 'v3'

class AnalyticsCoreReportingConnector(GoogleAPIConnector):
	"""AnalyticsCoreReportingConnector Derived Class extending GoogleAPIConnector Base Class."""

	_api_name = 'analyticsreporting'
	_default_api_version = 'v4'
	_discovery_uri = 'https://analyticsreporting.googleapis.com/$discovery/rest'

	def connect(self) -> object:
		"""Connector to build service for the api calls.

		Returns:-
			service (object): service/connector to initiate api calls.
		"""

		service = self._build_service(self._api_name, self._default_api_version, discovery_uri=self._discovery_uri)
		return service

class AnalyticsRealTimeConnector(GoogleAPIConnector):
	"""AnalyticsRealTimeConnector Derived Class extending GoogleAPIConnector Base Class."""

	_api_name = 'analytics'
	_default_api_version = 'v3'

class AnalyticsManagementConnector(GoogleAPIConnector):
	"""AnalyticsManagementConnector Derived Class extending GoogleAPIConnector Base Class."""

	_api_name = 'analytics'
	_default_api_version = 'v3'

class MerchantCenterConnector(GoogleAPIConnector):
	"""MerchantCenterConnector Derived Class extending GoogleAPIConnector Base Class."""

	_api_name = 'content'
	_default_api_version = 'v2.1'

class GoogleAPIConnectorFactory:

	_connectors = {
		"SPREADSHEET": SpreadsheetConnector,
		"GMAIL": GmailConnector,
		"DRIVE": DriveConnector,
		"MERCHANT_CENTER": MerchantCenterConnector,
		"ANALYTICS_REPORTING": AnalyticsCoreReportingConnector,
		"ANALYTICS_REAL_TIME": AnalyticsRealTimeConnector,
		"ANALYTICS_MANAGEMENT":AnalyticsManagementConnector
	}

	def __init__(self, credentials_file_path, auth_token_file_path, api_version:str=None):
		self.credentials_file_path = credentials_file_path
		self.auth_token_file_path = auth_token_file_path
		self.api_version = api_version

	def get_connector(self, connector_type):
		connector = None
		connector_class = None

		args = (self.credentials_file_path, self.auth_token_file_path)
		kwargs = {'api_version':self.api_version}

		if connector_type in self._connectors:
			connector_class = self._connectors[connector_type]
			connector = connector_class(*args, **kwargs)

		else:
			raise Exception("Please provide valid type of Google API connector")

		return connector

class GoogleAdsAPIConnector:

	"""Class for Google Advertising API client creation"""

	_valid_api_versions = ["v10","v9","v8", "v7", "v6"]

	def __init__(self, google_yaml_path:str, api_version:str):
		"""Parameterised constructor.

		Args:-
			google_yaml_path (str): google-ads.yaml file path
			api_version (str): valid api version for api access.
		"""

		self.google_yaml_path = google_yaml_path
		self.api_version = api_version

		if self.api_version not in self._valid_api_versions:
			raise ValueError(f'Version {self.api_version} not supported. Currently supported version : {self._valid_api_versions}')

	def connect(self) -> object:
		"""Function to get client.

		Returns:-
			client (object): client object to run api services.
		"""

		client = GoogleAdsClient.load_from_storage(path=self.google_yaml_path, version=self.api_version)

		return client

class FacebookAdsAPIConnector:
	"""Class for facebook credentials access to activate session."""

	def __init__(self, credentials_file_path:str):
		"""Parameterised constructor.

		Args:-
			credentials_file_path (str): facebook_credentials.json file path
		"""

		self.credentials_file_path = credentials_file_path

	def connect(self):
		"""Function to read credentials and activate facebook session."""

		with open(self.credentials_file_path) as f:
			data = json.load(f)

			access_token = data['long_lived_access_token']
			app_id = data['app_id']
			app_secret = data['app_secret']

		session = FacebookSession(app_id,app_secret,access_token)
		api = FacebookAdsApi(session)
		FacebookAdsApi.set_default_api(api)

class SlackAPIConnector:
	"""Class for slack credentials access."""

	def __init__(self, credentials_file_path:str):
		"""Parameterised constructor.

		Args:-
			credentials_file_path (str): slack_credentials.json file path
		"""

		self.credentials_file_path = credentials_file_path

	def connect(self) -> str:
		"""Function to read slack credentials and get access token,

		Returns:-
			keywordio_bot_access_token (str): access token to initiate
			slack api calls.
		"""

		with open(self.credentials_file_path) as f:
			data = json.load(f)
			keywordio_bot_access_token = data['Bot_User_OAuth_Access_Token']

		return keywordio_bot_access_token

def main():

	credentials_file_path = "C:\\Users\\Dell\\CREDENTIALS\\credentials.json"
	auth_token_file_path = "C:\\Users\\Dell\\CREDENTIALS\\vishwaja_chaudhari_token.json"

	# Option 1
	gmail_connector = GmailConnector(credentials_file_path, auth_token_file_path, api_version='v1')
	service = gmail_connector.connect()

	analytics_connector = AnalyticsCoreReportingConnector(credentials_file_path, auth_token_file_path)
	service = analytics_connector.connect()

	spreadheet_connector = SpreadsheetConnector(credentials_file_path, auth_token_file_path)
	service = spreadheet_connector.connect()

	# Option 2 (Preferred option)
	# connector_factory = GoogleAPIConnectorFactory(credentials_file_path, auth_token_file_path)
	# connector = connector_factory.get_connector('SPREADSHEET')
	# service = connector.connect()
	# print(f'connector: {connector}')

	spreadsheetId = '1hoPoTuSvw_ZVpnA6RBxyYt3CSrdX1k0JU4sgYNvxWjE'
	rangeName = 'Sheet1!A1:E'
	result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId,range=rangeName).execute()
	values = result.get('values', [])

	if not values:
		print('No data found.')
	else:
		print('Name, Major:')
		for row in values:
			print(row)

	google_ads_yaml_path = 'path/to/google-ads.yaml'
	google_ads_api_version = 'v7'

	connector = GoogleAdsAPIConnector(google_ads_yaml_path, google_ads_api_version)
	client = connector.connect()
	print(client)

if __name__ == '__main__':
	main()

