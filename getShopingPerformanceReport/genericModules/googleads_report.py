# Standard library imports
import time
import json
import sys

# Third party imports
from google.ads.google_ads.errors import GoogleAdsException
from google.protobuf import json_format
import json_flatten
import pandas as pd
import asyncio

# Local application imports

_MILLION = int(1e6)
_SUPPORTED_VERSION = ('v6',)
_DEFAULT_PAGE_SIZE = 1000

class GoogleAdsReport:
	
	"""GoogleAds API class"""

	def __init__(self, api_version:str, client:object, customer_id:str=None, use_stream:bool=True, page_size:int=None):
		"""Parameterized constructor"""

		self.api_version = api_version
		self.client = client
		self.customer_id = customer_id.replace('-','')
		self.use_stream = use_stream
		self.page_size = page_size
		self.ga_service = None

		if api_version not in _SUPPORTED_VERSION:
			raise ValueError(f'Version {api_version} not supported. Currently supported version : {_SUPPORTED_VERSION}')
		if not use_stream and not page_size:
			raise ValueError('Either set `page_size` or set `use_stream` to True')

	# customer_id
	@property
	def customer_id(self) -> str:
		"""Returns the customer_id"""

		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_:str):
		"""Set the customer_id."""
		# TODO: convert customer_id to hyphen format
		self.__customer_id = customer_id_.replace('-','')
  
	def build_query(self, fields:str, resource_name:str, conditions:str="", sort_by:str="", limit:int=None):
		"""This function builds google ads query.
  
		Args:-
		fields: String of fields to fetch.
		resource_name: String having type of report.
		conditions: String having conditions for the report.
		sort_by: String having the data to sort by certain field.
		limit: Integer value having number of rows to be fetched.
  
		Returns:-
		query: String of google ads query formed to fetch the report.
		"""

		query = f"""SELECT {fields} FROM {resource_name} """

		if conditions:
			query += f"""WHERE {conditions} """
   
		if sort_by:
			query += f"""ORDER BY {sort_by} """

		if limit:
			limit = int(limit)
			query += f"""LIMIT {limit} """
   
		return query

	def download(self, query:str, money_to_micros:list=[], include_roas:bool=False, require_resource_names:bool=False) -> "dataframe":
		"""This function downloads the google ads report using GoogleAds 
		API, converts the GoogleAdsRow to DataFrame and preprocesses the
		DataFrame.

		Args:-
		query: String containing the google ads query having selected,
			filterable, sortable fields.
		money_to_micros: List containing the name of the columns to be
			converted into micros i.e. divide by million value.
		include_roas: Boolean value indicating whether the dataframe 
			should contain the revenue on ad spend (roas) column.

		Returns:-
		df: A clean dataframe having the output of the query. 
		"""
		client = self.client
		customer_id = self.customer_id
		api_version = self.api_version
		page_size = self.page_size
		ga_service = self.ga_service

		print(f'downloading report for query {query} and customer_id: {customer_id}')
		print('Please wait...')
		start = time.time()

		if not ga_service:
			ga_service = client.get_service("GoogleAdsService", version=api_version)
			self.ga_service = ga_service

		report_df = pd.DataFrame()

		if self.use_stream:
			response = ga_service.search_stream(customer_id, query=query)
			report_df = self.__search_stream_to_df_conversion(response)
		else:
			response = ga_service.search(customer_id, query=query, page_size=page_size)
			report_df = self.__search_to_df_conversion(response)

		if not report_df.empty:
			report_df = self.preprocess_dataframe(report_df, money_to_micros, include_roas, require_resource_names)

		print('time required = ', time.time() - start)

		print('data dowloaded', report_df.shape)

		print('downloaded data')
		print(report_df)

		return report_df

	def preprocess_dataframe(self, df:"dataframe", money_to_micros:list, include_roas:bool, require_resource_names:bool) -> "dataframe":
		"""This function cleans the dataframe by changing the datatype 
		of the str columns to required float and int types, divides 
		asked metrics to million and calculates revenue on ad spend(roas)
		if true.

		Args :-
		df : A dataframe containing query output data.
		money_to_micros: List containing the name of the columns to be
			converted into micros i.e. divide by million value.
		include_roas: Boolean value indicating whether the dataframe 
			should contain the revenue on ad spend (roas) column.

		Returns:-
		df: preprocessed dataframe.
		"""

		df_columns = df.columns

		for column in df_columns:
			if money_to_micros and column in money_to_micros:
				df[column] = df[column].fillna(0)
				df[column] = df[column].apply(lambda value: float(value)/ _MILLION)
			if 'metrics' in column or 'micros' in column:
				df[column] = df[column].fillna(0)
				df[column] = df[column].apply(lambda value: round(float(value),2) if '.' in str(value) else int(str(value).replace('{}','0')))
			else:
				df[column] = df[column].fillna('--')

			if not require_resource_names and 'resource_name' in column:
				del df[column]

		# add roas
		if include_roas is True:
			if 'metrics.conversions_value' in df_columns and 'metrics.cost_micros'in df_columns:
				def calculate_roas(total_conv_value, cost):
					roas = round(total_conv_value / cost,2) if cost > 0 else 0.0
					return roas
				df['metrics.roas'] = df[['metrics.conversions_value', 'metrics.cost_micros']].apply(lambda values: calculate_roas(float(values[0]), float(values[1])), axis=1)
		return df

	def create_label_if_needed(self, label_name:str) -> str:
		"""This function fetches label id if label exists else creates
		label returns label id.

		Args:-
		label_name: Name of the label to retrieve ID or create and 
		retrieve id.

		Returns:-
		label_id: ID of the label fetched or created.

		Response samples:-
		label_results = 
		label {
			resource_name: "customers/4861039205/labels/12672795513"
			id: 12672795513
			name: "sample_label"
		}
		label_create_response = 
		results {
			resource_name: "customers/4861039205/labels/12672795513"
		}
		"""

		api_version = self.api_version
		client = self.client
		customer_id = self.customer_id

		ga_service = client.get_service("GoogleAdsService", version=api_version)
		query = """SELECT label.resource_name, label.name, label.id FROM label WHERE label.name='{}' AND label.status = 'ENABLED'""".format(label_name)

		label_results = ga_service.search_stream(customer_id, query=query)
		df = self.__search_stream_to_df_conversion(label_results)
		if not df.empty:
			label_resource_name = str(df['label.resource_name'].tolist()[0])
			label_id = str(df['label.id'].tolist()[0])
		else:
			operations= []
			label_service = client.get_service("LabelService", version=api_version)
			label_operation = client.get_type("LabelOperation", version=api_version)

			new_label = label_operation.create
			new_label.name = label_name
			new_label.status = client.get_type("LabelStatusEnum", version=api_version).ENABLED

			operations.append(label_operation)

			label_create_response = label_service.mutate_labels(customer_id,operations)
			if label_create_response:
				label_resource_name = label_create_response.results[0].resource_name
				label_id = str(label_resource_name.split('/')[-1])
			else: 
				label_id = ""

		return label_resource_name, label_id

	def python_list_to_googleads_list(self, data_list:list) -> tuple:

		data_string = ",".join(tuple(map (lambda element: f"'{element}'", data_list)))

		return data_string

	def __search_stream_to_df_conversion(self, response:object) -> "dataframe":
		"""This function converts the search stream response google ads rows to dataframe.

		Args:-
		response: GoogleAdsService.SearchStream response object

		Returns:-
		df: A dataframe having the output of the query.
		"""

		result_list = []
		for batch in response:
			for row in batch.results: 
				self.__json_to_flatten(row, result_list)
		df = pd.DataFrame(result_list)

		return df

	def __search_to_df_conversion(self, response:object) -> "dataframe":
		"""This function converts the search pages response google ads rows to dataframe.

		Args:-
		response: GoogleAdsService.Search response object

		Returns:-
		df: A dataframe having the output of the query.
		"""

		result_list = []
		for row in response : 
			self.__json_to_flatten(row, result_list)
		df = pd.DataFrame(result_list)

		return df

	def __json_to_flatten(self, row:"googleAdsRow", result_list:list):
		"""This function converts the datatype googleAdsRow to Json and
		flattens the json level to 1.

		Args:-
		row: A googleAdsRow from the result dictionary.
		result_list: An empty list to store flatten dictionary to 
			convert it into dataframe.
		"""

		json_str = json_format.MessageToJson(row, preserving_proto_field_name=True)
		data = json.loads(json_str)
		flatten_data = json_flatten.flatten(data)
		flatten_data = {key.replace('$float','').strip():value for key,value in flatten_data.items()}
		result_list.append(flatten_data)

class GoogleAdsBatchProcess:
	
	def __init__(self, api_version:str, client:object, customer_id:str=None):
	
		self.api_version = api_version
		self.client = client
		self.customer_id = customer_id

		if api_version not in _SUPPORTED_VERSION:
			raise ValueError(f'Version {api_version} not supported. Currently supported version : {_SUPPORTED_VERSION}')

	@property
	def customer_id(self) -> str:
		"""Returns the customer_id"""

		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_:str):
		"""Set the customer_id."""
  
		self.__customer_id = customer_id_
  
	async def bulk_mutate_initializer(self, operation_type:str, list_of_operations:list):
		"""Main function that runs the batch processing.
  		Args:
			operation_type: a str of the operation type corresponding to a field on
				the MutateOperation message class.
			list_of_operations: list of opeartions to mutate.
   		"""
  
		operations = []
		client = self.client
		customer_id = self.customer_id
		api_version = self.api_version

		batch_job_service = client.get_service("BatchJobService", version=api_version)
		batch_job_operation = self.__create_batch_job_operation()
		resource_name = self.__create_batch_job( batch_job_service, batch_job_operation)
		operations = operations + [
			self.__build_mutate_operation(operation_type, operation) for operation in list_of_operations
		]
		print(operations)
		self.__add_all_batch_job_operations(batch_job_service, operations, resource_name)
		operations_response = self.__run_batch_job(batch_job_service, resource_name)
		# Create an asyncio.Event instance to control execution during the
		# asyncronous steps in _poll_batch_job. Note that this is not important
		# for polling asyncronously, it simply helps with execution control so we
		# can run _fetch_and_print_results after the asyncronous operations have
		# completed.
		_done_event = asyncio.Event()
		self.__poll_batch_job(operations_response, _done_event)
		# Execution will stop here and wait for the asyncronous steps in
		# _poll_batch_job to complete before proceeding.
		await _done_event.wait()

		self.__fetch_and_print_results(batch_job_service, resource_name)

	def __create_batch_job_operation(self) -> object:
		"""Created a BatchJobOperation and sets an empty BatchJob instance to
		the "create" property in order to tell the Google Ads API that we're
		creating a new BatchJob.

		Returns: a BatchJobOperation with a BatchJob instance set in the "create" property.
		"""
		client = self.client
		api_version = self.api_version
  
		batch_job_operation = client.get_type("BatchJobOperation", version=api_version)
		batch_job = client.get_type("BatchJob", version=api_version)
		batch_job_operation.create.CopyFrom(batch_job)
  
		return batch_job_operation

	def __create_batch_job(self, batch_job_service:object, batch_job_operation:object):
		"""Creates a batch job for the specified customer ID.

		Args:
			batch_job_service: an instance of the BatchJobService message class.
			batch_job_operation: a BatchJobOperation instance set to "create"

		Returns: a str of a resource name for a batch job.
		"""

		customer_id = self.customer_id
  
		try:
			response = batch_job_service.mutate_batch_job(
				customer_id, batch_job_operation
			)
			resource_name = response.result.resource_name
			print(f'Created a batch job with resource name "{resource_name}"')
			return resource_name
		except GoogleAdsException as exception:
			self.__handle_google_ads_exception(exception)
	
	def __build_mutate_operation(self, operation_type:str, operation:object) -> object:
		"""Builds a mutate operation with the given operation type and operation.
		Args:
			operation_type: a str of the operation type corresponding to a field on
				the MutateOperation message class.
			operation: an operation instance.
		Returns: a MutateOperation instance
		"""
  
		client = self.client
		api_version = self.api_version
  
		mutate_operation = client.get_type("MutateOperation", version=api_version)
		getattr(mutate_operation, operation_type).CopyFrom(operation)
  
		return mutate_operation

	def __add_all_batch_job_operations(self, batch_job_service:object, operations:list, resource_name:str):
		"""Adds all mutate operations to the batch job.
		As this is the first time for this batch job, we pass null as a sequence
		token. The response will contain the next sequence token that we can use
		to upload more operations in the future.
		Args:
			batch_job_service: an instance of the BatchJobService message class.
			operations: a list of a mutate operations.
			resource_name: a str of a resource name for a batch job.
		"""
		try:
			response = batch_job_service.add_batch_job_operations(
				resource_name, None, operations
			)

			print(
				f"{response.total_operations} mutate operations have been "
				"added so far."
			)

			# You can use this next sequence token for calling
			# add_batch_job_operations() next time.
			print(
				"Next sequence token for adding next operations is "
				f"{response.next_sequence_token}"
			)
		except GoogleAdsException as exception:
			self.__handle_google_ads_exception(exception)

	def __run_batch_job(self, batch_job_service:object, resource_name:str) -> object:
		"""Runs the batch job for executing all uploaded mutate operations.

		Args:
			batch_job_service: an instance of the BatchJobService message class.
			resource_name: a str of a resource name for a batch job.

		Returns: a google.api_core.operation.Operation instance.
		"""
		try:
			response = batch_job_service.run_batch_job(resource_name)
			print(
				f'Batch job with resource name "{resource_name}" has been '
				"executed."
			)
			return response
		except GoogleAdsException as exception:
			self.__handle_google_ads_exception(exception)

	def __poll_batch_job(self, operations_response:object, event:object):
		"""Polls the server until the batch job execution finishes.

		Sets the initial poll delay time and the total time to wait before time-out.

		Args:
			operations_response: a google.api_core.operation.Operation instance.
			event: an instance of asyncio.Event to invoke once the operations have
				completed, alerting the awaiting calling code that it can proceed.
		"""
		loop = asyncio.get_event_loop()

		def _done_callback(future):
			# The operations_response object will call callbacks from a daemon
			# thread so we must use a threadsafe method of setting the event here
			# otherwise it will not trigger the awaiting code.
			loop.call_soon_threadsafe(event.set)

		# operations_response represents a Long-Running Operation or LRO. The class
		# provides an interface for polling the API to check when the operation is
		# complete. Below we use the asynchronous interface, but there's also a
		# synchronous interface that uses the Operation.result method.
		# See: https://googleapis.dev/python/google-api-core/latest/operation.html
		operations_response.add_done_callback(_done_callback)

	def __fetch_and_print_results(self, batch_job_service:object, resource_name:str):
		"""Prints all the results from running the batch job.

		Args:
			batch_job_service: an instance of the BatchJobService message class.
			resource_name: a str of a resource name for a batch job.
		"""
		print(
			f'Batch job with resource name "{resource_name}" has finished. '
			"Now, printing its results..."
		)

		page_size = _DEFAULT_PAGE_SIZE
		# Gets all the results from running batch job and prints their information.
		batch_job_results = batch_job_service.list_batch_job_results(
			resource_name, page_size=page_size
		)

		for batch_job_result in batch_job_results:
			status = batch_job_result.status.message
			status = status if status else "N/A"
			result = batch_job_result.mutate_operation_response
			result = result if result.ByteSize() else "N/A"
			print(
				f"Batch job #{batch_job_result.operation_index} "
				f'has a status "{status}" and response type "{result}"'
			)

	def __handle_google_ads_exception(self, exception):
		"""Prints the details of a GoogleAdsException object.
		Args:
			exception: an instance of GoogleAdsException.
		"""
		print(
			f'Request with ID "{exception.request_id}" failed with status '
			f'"{exception.error.code().name}" and includes the following errors:'
		)
		for error in exception.failure.errors:
			print(f'\tError with message "{error.message}".')
			if error.location:
				for field_path_element in error.location.field_path_elements:
					print(f"\t\tOn field: {field_path_element.field_name}")
		sys.exit(1)

def main():

	"""GoogleAds API"""

	# GOOGLEADS_API_VERSION = 'v6'

	# import google.ads.google_ads.client 
	# from google.ads.google_ads.client  import GoogleAdsClient

	# google_ads_client = (
	# 	google.ads.google_ads.client.GoogleAdsClient.load_from_storage()
	# )

	# customer_id = '8858752180'

	# google_ads_report = GoogleAdsReport(GOOGLEADS_API_VERSION, google_ads_client, customer_id)


	# query = """SELECT campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,ad_group_criterion.keyword.match_type, ad_group_criterion.status,metrics.impressions,metrics.clicks,metrics.cost_micros,metrics.conversions,metrics.conversions_value,metrics.ctr,metrics.average_cpc,segments.date FROM keyword_view WHERE segments.date DURING YESTERDAY AND campaign.status='ENABLED' AND ad_group.status='ENABLED' AND metrics.conversions_value > 10 LIMIT 10"""

	# money_to_micros = ['metrics.cost_micros','metrics.average_cpc']
	# df = google_ads_report.download(query, money_to_micros, include_roas=True)
	# df.to_excel('sample.xlsx',index=None)

	####################################################################

	"""GoogleAds API call from logic"""

	# utility = Utility(__file__, cred_path='BASE')

	# global_variables_dict = utility.get_global_variables()
	# google_ads_api_version = global_variables_dict['GOOGLE_ADS_API_VERSION']

	# user_credentials = Credentials(utility, developer_name)
	# google_ads_client = user_credentials.get_google_ads_client()

	# customer_id = '8858752180'

	# google_ads_report = GoogleAdsReport(google_ads_api_version, google_ads_client, customer_id)

 	# sample_campaign_ids_list = [1234, 56789]
	# sample_campaign_ids_list = google_ads_report.python_list_to_googleads_list(sample_campaign_ids_list)
 
	# query = f"""SELECT campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,ad_group_criterion.keyword.match_type, ad_group_criterion.status,metrics.impressions,metrics.clicks,metrics.cost_micros,metrics.conversions,metrics.conversions_value,metrics.ctr,metrics.average_cpc,segments.date FROM keyword_view WHERE segments.date DURING YESTERDAY AND campaign.status='ENABLED' AND ad_group.status='ENABLED' AND metrics.conversions_value > 10 AND campaign.id IN ({sample_campaign_ids_list}) LIMIT 10"""
 
	# or

	# fields = """campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.status, metrics.impressions,metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.ctr,metrics.average_cpc, segments.date"""
	
	# resource_name = """keyword_view"""

	# conditions = f"""segments.date DURING YESTERDAY AND campaign.status='ENABLED' AND ad_group.status='ENABLED' AND metrics.conversions_value > 10 AND campaign.id IN ({sample_campaign_ids_list})"""
 
	# limit = 10
 
	# query = google_ads_report.build_query(fields, resource_name, conditions=conditions, limit=limit)

	# money_to_micros = ['metrics.cost_micros','metrics.average_cpc']

	# df = google_ads_report.download(query, money_to_micros, include_roas=True)

if __name__ == '__main__':
	main()
