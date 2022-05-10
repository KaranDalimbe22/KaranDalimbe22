# Standard library imports
import time
import json
import sys
import asyncio

# Third party imports
from google.ads.googleads.errors import GoogleAdsException
import proto
import json_flatten
import pandas as pd

# Local application imports

_MILLION = int(1e6)
_DEFAULT_PAGE_SIZE = 1000
_REPORT_LEVELS = {
	'account':{
		'report_level_label_resource_column':'customer.labels'
	},
	'campaign':{
		'report_level_label_resource_column':'campaign.labels'
	},
	'ad_group':{
		'report_level_label_resource_column':'ad_group.labels'
	},
	'keyword':{
		'report_level_label_resource_column':'ad_group_criterion.labels'
	},
	'ad':{
		'report_level_label_resource_column':'ad_group_ad.labels'
	}
}


class GoogleAdsReport:

	"""GoogleAds API class"""

	def __init__(self, client:object, customer_id:str=None, use_stream:bool=True, page_size:int=None):
		"""Parameterized constructor"""

		self.client = client
		self.customer_id = customer_id.replace('-','')
		self.use_stream = use_stream
		self.page_size = page_size
		self.ga_service = None

		if not use_stream and not page_size:
			raise ValueError('Either set page_size or set use_stream to True')

		if not self.page_size:
			self.page_size = _DEFAULT_PAGE_SIZE

	@property
	def customer_id(self) -> str:
		"""Returns the customer_id"""

		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_:str):
		"""Set the customer_id."""
		# TODO: convert customer_id to hyphen format
		self.__customer_id = customer_id_.replace('-','')

	def python_list_to_googleads_list(self, data_list:list) -> tuple:

		data_string = ",".join(tuple(map (lambda element: f"'{element}'", data_list)))

		return data_string

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
		page_size = self.page_size
		ga_service = self.ga_service

		# print(f'downloading report for query {query} and customer_id: {customer_id}')
		# print('Please wait...')
		start = time.time()

		if not ga_service:
			ga_service = client.get_service("GoogleAdsService")
			self.ga_service = ga_service

		report_df = pd.DataFrame()

		if self.use_stream:
			search_stream_request = client.get_type("SearchGoogleAdsStreamRequest")
			search_stream_request.customer_id = customer_id
			search_stream_request.query = query
			response = ga_service.search_stream(request=search_stream_request)
			report_df = self.__search_stream_to_df_conversion(response)
		else:
			search_request = client.get_type("SearchGoogleAdsRequest")
			search_request.customer_id = customer_id
			search_request.query = query
			search_request.page_size = page_size
			response = ga_service.search(request=search_request)
			report_df = self.__search_to_df_conversion(response)

		if not report_df.empty:
			query_fields = query.split('FROM')[0].replace('SELECT','').replace('\n','').replace('\t','').strip().split(',')
			query_fields = [field.strip() for field in query_fields]
			report_df = self.preprocess_dataframe(query_fields, report_df, money_to_micros, include_roas, require_resource_names)

		# print('time required = ', time.time() - start)

		# print('data dowloaded', report_df.shape)

		# print('downloaded data')
		# print(report_df)

		return report_df

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

		json_str = proto.Message.to_json(row, use_integers_for_enums=False, preserving_proto_field_name=True, including_default_value_fields=False)
		data = json.loads(json_str)
		flatten_data = json_flatten.flatten(data)
		flatten_data = {key.split('$')[0].strip():value for key,value in flatten_data.items()}
		result_list.append(flatten_data)

	def preprocess_dataframe(self, query_fields:list, df:"dataframe", money_to_micros:list, include_roas:bool, require_resource_names:bool) -> "dataframe":
		"""This function cleans the dataframe by changing the datatype 
		of the str columns to required float and int types, divides 
		asked metrics to million and calculates revenue on ad spend(roas)
		if true.

		Args :-
		query_fields : query fields to create columns not fetched in
			report.
		df : A dataframe containing query output data.
		money_to_micros: List containing the name of the columns to be
			converted into micros i.e. divide by million value.
		include_roas: Boolean value indicating whether the dataframe 
			should contain the revenue on ad spend (roas) column.

		Returns:-
		df: preprocessed dataframe.
		"""

		df_columns = list(df.columns)

		for column in df_columns:
			if (money_to_micros and column in money_to_micros) or ('_micros' in column):
				df[column] = df[column].fillna(0)
				df[column] = df[column].apply(lambda value: float(value)/ _MILLION)
			if 'metrics' in column or 'micros' in column:
				df[column] = df[column].fillna(0)

				def convert_datatypes(value):
					if '.' in str(value):
						value = float(value)
					elif str(value).isnumeric():
						value = int(value)
					else:
						value = str(value)
					return value

				df[column] = df[column].apply(lambda value: convert_datatypes(value))
			else:
				df[column] = df[column].fillna('--')

			if not require_resource_names and 'resource_name' in column:
				del df[column]

		# add roas
		if include_roas is True:
			if 'metrics.conversions_value' in df_columns and 'metrics.cost_micros'in df_columns:
				def calculate_roas(total_conv_value, cost):
					roas = total_conv_value / cost if cost > 0 else 0.0
					return roas
				df['metrics.roas'] = df[['metrics.conversions_value', 'metrics.cost_micros']].apply(lambda values: calculate_roas(float(values[0]), float(values[1])), axis=1)

		# missing columns in fetched report
		missing_query_fields_in_report = [query_field for query_field in query_fields if query_field not in df_columns]
		if missing_query_fields_in_report:
			for missing_query_field in missing_query_fields_in_report:
				for report_field in df_columns:
					if missing_query_field in report_field:
						missing_query_fields_in_report.remove(missing_query_field)
						break
			if missing_query_fields_in_report:
				for missing_field in missing_query_fields_in_report:
					if '.labels' not in missing_field:
						df[missing_field] = 0

		return df

	def fetch_label(self, label_name:str) -> (str, str):

		label_resource_name = None
		label_id = None

		df = self._get_label_performance(label_name=label_name)

		if df.empty:
			return label_resource_name, label_id
		label_resource_name = str(df['label.resource_name'].tolist()[0])
		label_id = str(df['label.id'].tolist()[0])

		print('\nExisting Label Details:-')
		print('label_name = ', label_name)
		print('label_resource_name = ', label_resource_name)
		print('label_id = ', label_id)

		return label_resource_name, label_id

	def requested_labels_to_report(self, report_df:"dataframe", report_level:str, label_names:list, operator:str="CONTAINS ANY") -> "dataframe":

		requested_labels_report_df = pd.DataFrame()

		_valid_operators = ['CONTAINS ALL', 'CONTAINS ANY', 'CONTAINS NONE']

		if operator not in _valid_operators:
			print(f'Invalid Operator!! Please use any one amongst {_valid_operators}')
			return requested_labels_report_df

		labels_report_df = self.labels_to_report(report_df, report_level)

		if labels_report_df.empty:
			return requested_labels_report_df

		if (not labels_report_df.empty) and (label_names):

			def check_label(row, label_names):
				if all(label_name in row for label_name in label_names):
					condition_matched = 'CONTAINS ALL'
				elif any(label_name in row for label_name in label_names):
					condition_matched = 'CONTAINS ANY'
				else:
					condition_matched = 'CONTAINS NONE'

				return condition_matched

			labels_report_df['condition_matched'] = labels_report_df['label.name'].apply(lambda row: check_label(row, label_names))

			requested_labels_report_df = labels_report_df[labels_report_df['condition_matched'] == operator.upper()].copy()

			del labels_report_df['condition_matched']
			del requested_labels_report_df['condition_matched']

		# print("requested labels merged to report = ")
		# print(requested_labels_report_df)

		return requested_labels_report_df

	def labels_to_report(self, report_df:"dataframe", report_level:"str") -> "dataframe":

		labels_dict = {}
		labels_df = self._get_label_performance()
		if not labels_df.empty:
			# label_resource_name:label_name
			labels_dict = {label_data[0]:[label_data[1],label_data[2]] for label_data in labels_df.to_dict('split')['data']}

		report_level_label_resource_column = _REPORT_LEVELS.get(report_level).get('report_level_label_resource_column')

		if (report_df.empty) or (not labels_dict):
			print("Error!! Empty report_df or labels_dict.")
			return report_df

		report_df_labels_columns = [str(column) for column in report_df.columns if report_level_label_resource_column in column]

		if report_df_labels_columns:

			def merge_label_resource_names(row):

				merged_list = row.values.tolist()
				merged_list = [value for value in merged_list if value != '--']
				if not merged_list:
					merged_list = ['--']
				return merged_list

			report_df['label.resource_name'] = report_df[report_df_labels_columns].apply(lambda row: merge_label_resource_names(row), axis=1)

			report_df.drop(report_df_labels_columns, axis = 1, inplace = True)

			def get_label_data_from_label_performance(resource_names, index):

				label_names = []
				if resource_names != ['--'] :
					for resource_name in resource_names:
						label_names.append(labels_dict[resource_name][index])
				else:
					label_names = ['--']

				return label_names

			report_df['label.id'] = report_df['label.resource_name'].apply(lambda resource_names: get_label_data_from_label_performance(resource_names,0))
			report_df['label.name'] = report_df['label.resource_name'].apply(lambda resource_names: get_label_data_from_label_performance(resource_names,1))

		else:
			label_data = "['--']"
			report_df['label.resource_name'] = label_data
			report_df['label.id'] = label_data
			report_df['label.name'] = label_data

		del report_df['label.resource_name']

		# print("all labels merged to report = ")
		# print(report_df)

		return report_df

	def _get_label_performance(self, label_name:str=None) -> "dataframe":

		fields = """label.resource_name, label.id, label.name"""

		resource_name = f"""label"""

		conditions = """label.status IN ('ENABLED','REMOVED','UNKNOWN')"""

		if label_name:
			conditions += f""" AND label.name='{label_name}'"""

		query = self.build_query(fields, resource_name, conditions=conditions)

		money_to_micros = []

		df = self.download(query, money_to_micros, include_roas=False, require_resource_names=True)

		if not df.empty:
			df = df.drop_duplicates()

		return df


class GoogleAdsMutate:

	def __init__(self, client:object, customer_id:str=None):

		self.client = client
		self.customer_id = customer_id.replace('-','')

	@property
	def customer_id(self) -> str:
		"""Returns the customer_id"""

		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_:str):
		"""Set the customer_id."""

		self.__customer_id = customer_id_.replace('-','')

	def create_label(self, label_name:str) -> (str, str):
		"""This function fetches label id if label exists else creates
		label returns label id.

		Args:-
		label_name: Name of the label to retrieve ID or create and 
		retrieve id.

		Returns:-
		label_resource_name: Resource name of the label 
			fetched or created.
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

		client = self.client
		customer_id = self.customer_id

		label_resource_name = None
		label_id = None

		google_ads_report = GoogleAdsReport(client, customer_id)

		label_resource_name, label_id = google_ads_report.fetch_label(label_name)

		if (label_resource_name and label_id):
			print('Existing Label Details:-')
			print('label_name = ', label_name)
			print('label_resource_name = ', label_resource_name)
			print('label_id = ', label_id)
			return label_resource_name, label_id

		operations= []
		label_service = client.get_service("LabelService")
		label_operation = client.get_type("LabelOperation")

		new_label = label_operation.create
		new_label.name = label_name
		new_label.status = client.get_type("LabelStatusEnum").LabelStatus.ENABLED

		operations.append(label_operation)
		print("\nLabel does not exists, therefore creating the label, Please wait...\n")
		label_create_response = label_service.mutate_labels(customer_id=customer_id, operations=operations)

		if not label_create_response:
			return label_resource_name, label_id

		label_resource_name = label_create_response.results[0].resource_name
		label_id = str(label_resource_name.split('/')[-1])

		print('Created Label Details:-')
		print('label_name = ', label_name)
		print('label_resource_name = ', label_resource_name)
		print('label_id = ', label_id)

		return label_resource_name, label_id


class GoogleAdsBatchProcess:

	def __init__(self, client:object, customer_id:str=None):

		self.client = client
		self.customer_id = customer_id.replace('-','')

	@property
	def customer_id(self) -> str:
		"""Returns the customer_id"""

		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_:str):
		"""Set the customer_id."""

		self.__customer_id = customer_id_.replace('-','')

	async def bulk_mutate_initializer(self, operation_type:str, mutate_operations:list):
		"""Main function that runs the batch processing.
		Args:
			operation_type: a str of the operation type corresponding to a field on
				the MutateOperation message class.
			mutate_operations: list of opeartions to mutate.
		"""

		operations = []
		client = self.client
		customer_id = self.customer_id

		batch_job_service = client.get_service("BatchJobService")
		batch_job_operation = self.__create_batch_job_operation()
		resource_name = self.__create_batch_job( batch_job_service, batch_job_operation)
		operations = operations + [
			self.__build_mutate_operation(operation_type, operation) for operation in mutate_operations
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

		batch_job_operation = client.get_type("BatchJobOperation")
		batch_job = client.get_type("BatchJob")
		client.copy_from(batch_job_operation.create, batch_job)

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
				customer_id=customer_id, operation=batch_job_operation
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

		mutate_operation = client.get_type("MutateOperation")
		# Retrieve the nested operation message instance using getattr then copy the
		# contents of the given operation into it using the client.copy_from method.
		client.copy_from(getattr(mutate_operation, operation_type), operation)

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
				resource_name=resource_name, sequence_token=None, mutate_operations=operations
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
			response = batch_job_service.run_batch_job(resource_name=resource_name)
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
			event: an instance of asyncio.Event to invoke once the operations have completed, alerting the awaiting calling code that it can proceed.
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

		list_results_request = self.client.get_type("ListBatchJobResultsRequest")
		list_results_request.resource_name = resource_name
		list_results_request.page_size = _DEFAULT_PAGE_SIZE
		# Gets all the results from running batch job and prints their information.
		batch_job_results = batch_job_service.list_batch_job_results(
			request=list_results_request
		)

		for batch_job_result in batch_job_results:
			status = batch_job_result.status.message
			status = status if status else "N/A"
			result = batch_job_result.mutate_operation_response
			result = result or "N/A"
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

	"""GoogleAds API Report Class"""

	GOOGLEADS_API_VERSION = 'v7'

	import google.ads.googleads.client 
	from google.ads.googleads.client  import GoogleAdsClient

	google_ads_client = GoogleAdsClient.load_from_storage(version=GOOGLEADS_API_VERSION)

	customer_id = '8858752180'

	google_ads_report = GoogleAdsReport(google_ads_client, customer_id)

	#EG: 1.

	query = """SELECT campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,ad_group_criterion.keyword.match_type, ad_group_criterion.status,metrics.impressions,metrics.clicks,metrics.cost_micros,metrics.conversions,metrics.conversions_value,metrics.ctr,metrics.average_cpc,segments.date FROM keyword_view WHERE segments.date DURING LAST_14_DAYS AND campaign.status='ENABLED' AND ad_group.status='ENABLED' AND metrics.conversions_value > 0"""

	money_to_micros = ['metrics.cost_micros','metrics.average_cpc']
	df = google_ads_report.download(query, money_to_micros, include_roas=True)
	df.to_excel('sample.xlsx',index=None)

	#EG: 2.

	fields = """campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.status, metrics.impressions,metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.ctr,metrics.average_cpc, segments.date"""

	resource_name = """keyword_view"""

	conditions = f"""segments.date DURING YESTERDAY AND campaign.status='ENABLED' AND ad_group.status='ENABLED' AND metrics.conversions_value > 10 AND campaign.id IN ({sample_campaign_ids_list})"""

	limit = 10

	query = google_ads_report.build_query(fields, resource_name, conditions=conditions, limit=limit)

	money_to_micros = ['metrics.cost_micros','metrics.average_cpc']

	df = google_ads_report.download(query, money_to_micros, include_roas=True)

	################### label Reporting and Mutate ####################
	label_names = ['NO_BO']
	requested_df = google_ads_report.requested_labels_to_report(
			df, 
			'campaign', 
			label_names,
			operator="CONTAINS ALL"
		)

	all_df = google_ads_report.labels_to_report(
			df, 
			'campaign', 
		)

	label_resource_name, label_id = google_ads_report.fetch_label('NO_BO')

	google_ads_mutate = GoogleAdsMutate(client, customer_id)
	label_resource_name, label_id = google_ads_mutate.create_label('TEST')

	####################################################################

	"""GoogleAds API Batch Process Class"""

	GOOGLEADS_API_VERSION = 'v7'

	import google.ads.googleads.client 
	from google.ads.googleads.client  import GoogleAdsClient
	from google.api_core import protobuf_helpers

	google_ads_client = GoogleAdsClient.load_from_storage(version=GOOGLEADS_API_VERSION)

	customer_id = '4861039205'
	ad_group_id = '1775618080'
	pause_adgroups = []

	google_ads_batch_process = GoogleAdsBatchProcess(google_ads_client, customer_id)

	ad_group_service = google_ads_client.get_service("AdGroupService")

	ad_group_operation = google_ads_client.get_type("AdGroupOperation")
	ad_group = ad_group_operation.update
	ad_group.resource_name = ad_group_service.ad_group_path(
		customer_id, ad_group_id
	)
	ad_group.status = google_ads_client.get_type("AdGroupStatusEnum").AdGroupStatus.PAUSED
	google_ads_client.copy_from(
		ad_group_operation.update_mask,
		protobuf_helpers.field_mask(None, ad_group._pb),
	)

	pause_adgroups.append(ad_group_operation)

	asyncio.run(google_ads_batch_process.bulk_mutate_initializer('ad_group_operation', pause_adgroups))


if __name__ == '__main__':
	main()
