# Standard library imports
import time

# Third party imports
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# Local application imports


class FacebookAdsReport:

	"""Facebook Business API class"""

	def __init__(self):
		"""Parameterized constructor"""

		pass

	def download(self, account_id:str, fields:list, params:dict, report_function:str='get_insights') -> "dataframe":
		"""This function downloads the facebook insights report using Facebook Ads Insights API, converts the response to DataFrame.

		Args:-
		account_id: ad account id.
		fields: insights report fields.
		params: predicates and date range.

		Returns:-
		df: A clean dataframe having the output of the requested report. 
		"""

		print(f'downloading report for requested data = {fields} {params}')
		print('Please wait...')
		start = time.time()

		report_level_object = AdAccount(account_id)
		insights = eval(f'report_level_object.{report_function}(fields=fields, params=params)')

		report_df = self._to_df(insights)

		print('time required = ', time.time() - start)

		print('data dowloaded', report_df.shape)

		print('downloaded data')
		print(report_df)

		return report_df

	def _to_df(self, insights:list) -> "dataframe":

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

		return df

def main():

	credentials_file_path = "your cred path"

	import os,sys
	sys.path.append(os.environ['SHARED_PACKAGE_PATH'])
	from genericModules.api_connectors import FacebookAdsAPIConnector

	connector = FacebookAdsAPIConnector(credentials_file_path)
	connector.connect()
	print(f'connector: {connector}')

	account_id = 'act_35134070'

	facebook_report = FacebookAdsReport()

	# EG 1: Insights report
	fields =  [ 
			AdsInsights.Field.account_id,
			AdsInsights.Field.account_name,
			AdsInsights.Field.campaign_id,
			AdsInsights.Field.campaign_name,
			AdsInsights.Field.spend
	]
	params = {
		'level' : 'campaign',
		'date_preset': 'last_7d'
	}

	df = facebook_report.download(account_id, fields, params)

	# EG 2: Campaigns report
	fields =  [ 
		'id',
		'name',
		'daily_budget'
	]
	params = {
		'date_preset': 'last_7d'
	}

	df = facebook_report.download(account_id, fields, params, report_function='get_campaigns')

if __name__ == '__main__':
	main()
