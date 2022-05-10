# Standard library imports
from typing import List
import datetime
import io
import copy
import time

# Third party imports
from googleads import adwords
import pandas as pd

# Local application imports


_reports_without_zero_impression = [
	'SEARCH_QUERY_PERFORMANCE_REPORT',
	'GEO_PERFORMANCE_REPORT',
	'KEYWORDLESS_QUERY_REPORT',
	'SHOPPING_PERFORMANCE_REPORT'
]
_report_column_types = {
	'xmlAttribute': 'xmlAttributeName',
	'displayName': 'displayFieldName',
	'fieldName': 'fieldName'
}
_default_column_type = 'xmlAttribute'

_MILLION = int(1e6)

class GoogleAdsReport:


	def __init__(self, client, report_type, api_version, customer_id=None, report_downloader=None):

		# public attributes
		self.client = client
		self.api_version = api_version
		self.customer_id = customer_id

		# private attributes with input value
		self.__report_type = report_type
		self.__report_downloader = report_downloader

		# private attributes with default value
		self.__fields = []
		self.__predicates = []
		self.__date_range = {
			'type': 'TODAY'
		}
		self.__optional_headers = {
			'skip_report_header': True,
			'skip_column_header': False,
			'skip_report_summary': True,
			'include_zero_impressions': True
		}
		self.__possible_fields = []

		if report_type in _reports_without_zero_impression:
			self.__optional_headers['include_zero_impressions'] = False


	# report_type
	@property
	def report_type(self):
		"""Returns the report type."""
		return self.__report_type


	# customer_id
	@property
	def customer_id(self):
		"""Returns the customer_id"""
		return self.__customer_id

	@customer_id.setter
	def customer_id(self, customer_id_):
		"""Set the customer_id."""
		# TODO: convert customer_id to hyphen format
		self.__customer_id = customer_id_


	# fields
	@property
	def fields(self):
		"""Returns report fields."""
		return self.__fields

	@fields.setter
	def fields(self, fields_):
		"""Sets the report fields."""
		self.__fields = fields_


	# date range
	# TODO: add validations
	@property
	def date_range(self):
		"""Returns date range of report."""
		return self.__date_range

	@date_range.setter
	def date_range(self, date_range_):
		"""Sets date range of report."""

		start_date = date_range_.get('start_date', '')
		end_date = date_range_.get('end_date', '')

		# if invalid date range type then raise error

		# TODO: add date validations
		if start_date and end_date:
			self.__date_range['type'] = 'CUSTOM_DATE'
			self.__date_range['start_date'] = start_date
			self.__date_range['end_date'] = end_date

		else:
			self.__date_range['type'] = date_range_.get('type', None)


	# predicates
	@property
	def predicates(self):
		"""Sets report predicates."""
		return self.__predicates

	@predicates.setter
	def predicates(self, predicates_):
		"""Sets report predicates."""
		# handle validation
		if not predicates_ or type(predicates_) != list:
			raise Exception('Predicates should be list')

		self.__predicates = predicates_


	# optional header
	@property
	def optional_headers(self):
		"""Returns the optional headers."""
		return self.__optional_headers

	@optional_headers.setter
	def optional_headers(self, optional_headers_):
		"""Sets the value of existing optional headers."""

		report_type = self.report_type
		for header, value in optional_headers_.items():

			# skip if the header does not exists
			if header not in self.__optional_headers:
				print('Invalid optional header', header)
				continue

			if type(value) != bool:
				print('header value should be of type boolean')
				continue

			# add check for include_zero_impressions
			if header == 'include_zero_impressions':
				if report_type in _reports_without_zero_impression:
					value = False

			self.__optional_headers[header] = value


	def get_possible_fields(self,
		behavior_types: list=[],
		field_types: list=[]):
		"""Returns all possible fields."""

		possible_fields = self.__possible_fields
		if not self.__possible_fields:
			report_definition_service = self.client.GetService(
			  'ReportDefinitionService', version=self.api_version)

			# Get report fields.
			fields = report_definition_service.getReportFields(self.report_type)

			fields = list(fields)
			possible_fields = []

			for field in fields:
				possible_fields.append(field)

			self.__possible_fields = possible_fields


		if behavior_types:
			possible_fields = list(filter(lambda field: \
										field['fieldBehavior'] in behavior_types
									, possible_fields))

		if field_types:
			possible_fields = list(filter(lambda field: \
										field['fieldType'] in field_types
									, possible_fields))

		return possible_fields

	def download(self,
		# output_format: str='xmlAttribute',
		output_column_type: str=_default_column_type,
		include_roas: bool=True) -> pd.DataFrame:
		"""Returns the downloaded report data.

		Args:
			self: instance			# implicit arguement

		Kwargs:
			output_column_type: type of columns in result DataFrame. Value options -> ['xmlAttribute', 'displayName', 'fieldName'].
			include_roas: if True then add roas column in DataFrame.
		"""

		if output_column_type not in _report_column_types:
			raise Exception(f'Invalid output_column_type: {output_column_type}. Possible values: ', list(_report_column_types.keys()))

		# initialize report_downloader if None
		if self.__report_downloader is None:
			self.__report_downloader = self.client.GetReportDownloader(version=self.api_version)


		report_downloader = self.__report_downloader
		date_range = self.date_range
		report_type = self.report_type
		predicates = self.predicates
		customer_id = self.customer_id
		optional_headers = self.optional_headers

		# default download format & column type
		download_format = 'CSV'
		input_column_type = 'displayName'

		# default return value
		df = pd.DataFrame()

		output = io.StringIO()
		report = {
			'reportName': report_type,
			'reportType': report_type,
			'downloadFormat': download_format,
			'selector': {
				'fields': self.fields,
			}
		}

		# get selector for easier access
		selector = report['selector']

		# add date range
		start_date = date_range.get('start_date', '')
		end_date = date_range.get('end_date', '')
		if start_date and end_date:
			selector['dateRange'] = {
				'min': start_date,
				'max': end_date
			}

			if report.get('dateRangeType') != 'CUSTOM_DATE':
				report['dateRangeType'] = 'CUSTOM_DATE'

		else:
			report['dateRangeType'] = date_range.get('type', None)

		# add predicates
		if predicates:
			selector['predicates'] = predicates

		print(f'downloading report {report_type} for customer_id: {customer_id}')
		print(report)
		print('Please wait...')
		start = time.time()
		# download report
		data = report_downloader.DownloadReport(
			report,
			output,
			client_customer_id=customer_id,
			**optional_headers
		)
		output.seek(0)
		df = pd.read_csv(output)
		print('time required = ', time.time() - start)

		print('data dowloaded', df.shape)


		# preprocess dataframe
		if not df.empty:
			# preprocess df
			df = self.preprocess_dataframe(df, include_roas)

			# rename columns
			df = self.rename_columns(df, input_column_type, output_column_type)

		print('downloaded data')
		print(df)
		return df

	def preprocess_dataframe(self, df, include_roas):
		"""Returns preprocessed dataframe."""

		columns = df.columns
		# add roas
		if include_roas is True:
			if 'Total conv. value' in columns and 'Cost' in columns:
				def calculate_roas(total_conv_value, cost):
					if cost > _MILLION:
						cost = cost / _MILLION

					roas = round(total_conv_value / cost, 2) if cost > 0 else 0.0
					return roas

				df['roas'] = df[['Total conv. value', 'Cost']].apply(lambda values: calculate_roas(values[0], values[1]), axis=1)

		# convert Impr Share columns to numeric
		for column in df.columns:
			if 'IS' in column or 'Impr. share' in column:
				df[column] =  df[column].str.replace('--','0').str.replace('%','')
				df[column] =  df[column].str.replace('< 10','10')
				df[column] =  df[column].str.replace('> 90','90')
				df[column] = df[column].apply(pd.to_numeric)

		# handle micro amount metric
		money_fields = self.get_possible_fields(field_types=['Money'])
		money_fields = tuple(field['displayFieldName'] for field in money_fields)
		for column in columns:
			if column in money_fields:
				df[column] = df[column].apply(lambda value: value/ _MILLION)

		return df

	def rename_columns(self, df, input_column_type, output_column_type):
		"""Returns a new dataframe with renamed columns."""

		if (input_column_type not in _report_column_types):
			raise Exception("Invalid input_column_type:", input_column_type)

		if (output_column_type not in _report_column_types):
			raise Exception("Invalid output_column_type:", output_column_type)

		input_column_key = _report_column_types.get(input_column_type, '')
		output_column_key = _report_column_types.get(output_column_type, '')
		possible_fields = self.get_possible_fields()
		columns_mapping = {
			field[input_column_key]: field[output_column_key]
			for field in possible_fields
		}
		df = df.rename(columns=columns_mapping)
		return df

	def get_copy(self):
		"""Return copy of GoogleAdsReport instance."""
		report = copy.copy(self)

		# assign new copy of client
		client = copy.copy(self.client)
		report.client = client

		# assign new copy of report_downloader
		report_downloader = copy.copy(self.__report_downloader)
		report.__report_downloader = report_downloader
		return report


# script code
API_VERSION = 'v201809'
def main():
	client = adwords.AdWordsClient.LoadFromStorage()
	fields = ['ExternalCustomerId', 'Cost', 'Clicks', 'ConversionValue', 'HeadlinePart1']
	date_range = {'type': 'LAST_7_DAYS'}
	report_type = 'AD_PERFORMANCE_REPORT'
	customer_id = '885-875-2180'
	keyword_report = GoogleAdsReport(client, report_type, API_VERSION)

	keyword_report.fields = fields
	keyword_report.date_range = date_range
	keyword_report.customer_id = customer_id
	keyword_report.optional_headers = {'include_zero_impressions': False}

	# uncoment to download
	# keyword_report.download()

	return keyword_report


if __name__ == '__main__':
	main()
