# Standard library imports
import time

# Third party imports
import pandas as pd

# Local application imports

class GoogleAnalyticsReport:

	"""GoogleAnalyticsReporting API class"""

	def __init__(self, service:object):
		"""Parameterized constructor"""

		self.service = service

	def download(self, report_request:dict) -> "dataframe":
		"""This function downloads the google analytics report using Google Analytics Reporting API, converts the response to DataFrame.

		Args:-
		service: analytics service through api connector to download report.
		report_request: report request body containing view/profile id, date ranges, metrics, dimensions and filters.

		Returns:-
		df: A clean dataframe having the output of the requested report. 
		"""

		service = self.service

		# print(f'downloading report for requested data = {report_request}')
		# print('Please wait...')
		start = time.time()

		body = {'reportRequests':[report_request]}
		response =  service.reports().batchGet(body=body).execute()

		report_df = pd.DataFrame()
		report_df = self._to_df(response)

		# print('time required = ', time.time() - start)

		# print('data dowloaded', report_df.shape)

		# print('downloaded data')
		# print(report_df)

		return report_df

	def _to_df(self, response:list) -> "dataframe":

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
		df = df.fillna(0)

		return df

def main():

	credentials_file_path = "your cred path"
	auth_token_file_path = "your token path" # please create token.json, do not use .dat file

	import os,sys
	sys.path.append(os.environ['SHARED_PACKAGE_PATH'])
	from genericModules.api_connectors import GoogleAPIConnectorFactory

	connector_factory = GoogleAPIConnectorFactory(credentials_file_path, auth_token_file_path)
	connector = connector_factory.get_connector('ANALYTICS_REPORTING')
	service = connector.connect()
	print(f'connector: {connector}')

	start_date = '2021-07-01'
	end_date = '2021-07-06'

	view_id = '11234117'

	report_request = {
		'viewId': str(view_id),
		'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
		'metrics':  [
			{'expression': 'ga:transactionRevenue'}
		],
		"dimensions": [
			{'name': 'ga:date'}
		],
		"pageSize": 100000

	}

	google_analytics_report = GoogleAnalyticsReport(service)
	df = google_analytics_report.download(report_request)

if __name__ == '__main__':
	main()
