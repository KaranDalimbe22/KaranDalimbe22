# Standard library imports
import os
import sys
import threading
import traceback
from datetime import datetime
import copy

#Third party imports
import pandas as pd

# local application imports
sys.path.append(os.environ['SHARED_PACKAGE_PATH'])
from genericModules.api_connectors import GoogleAdsAPIConnector
from genericModules.spreadsheet import Spreadsheet
from genericModules.google_ads_operations import GoogleAdsReport
from genericModules.google_ads_operations import GoogleAdsMutate
from genericModules.gmail import Gmail

# Global variables
GOOGLE_ADS_API_VERSION = 'v7'


def get_customer_ids(
	script_utility, 
	api_connector_factory
):

	connector = api_connector_factory.get_connector(
		'SPREADSHEET'
	)
	spreadsheet_service = connector.connect()

	sheet_ranges = {
		'Sheet1':['A:B']
	}

	bo_spreadsheet, customers_df = script_utility.get_BO_sheet_data(
		spreadsheet_service, 
		Spreadsheet, 
		sheet_ranges
	)

	customer_ids = customers_df['Customer Id'].tolist()

	return customers_df, customer_ids

def main(
	PRODUCTION_MODE, 
	script_utility, 
	api_connector_factory, 
	customers_df, 
	result_count, 
	errors
):

	google_ads_yaml_path = script_utility.get_google_ads_yaml_path()

	connector = GoogleAdsAPIConnector(
		google_ads_yaml_path, 
		GOOGLE_ADS_API_VERSION
	)
	google_ads_client = connector.connect()

	log_dict = {}

	call_threads(
		script_utility, 
		customers_df, 
		google_ads_client, 
		result_count, 
		errors, 
		log_dict
	)

	if log_dict:
		if PRODUCTION_MODE :
			receiver = ''
		else:
			receiver = ''
		send_email(
			api_connector_factory, 
			log_dict, 
			receiver
		)


	return result_count, errors

#---Function to create threads
def call_threads(
	PRODUCTION_MODE, 
	script_utility, 
	customers_df, 
	google_ads_client, 
	result_count, 
	errors, 
	log_dict
):

	threads = []

	for index,row in customers_df.iterrows():
		customer_id = row['Customer Id']
		account_name = row['Account Name']

		print('\nSCRIPT RUNNING FOR - ',str(customer_id))

		thread = myThread(
			PRODUCTION_MODE, 
			script_utility, 
			customer_id, 
			account_name, 
			google_ads_client, 
			result_count, 
			errors, 
			log_dict
		)

		thread.start()
		threads.append(
			thread
		)

	for thread in threads:
		thread.join()

class myThread(threading.Thread):
	_attributes = (
		'PRODUCTION_MODE', 
		'script_utility', 
		'customer_id', 
		'account_name', 
		'google_ads_client', 
		'result_count', 
		'errors', 
		'log_dict'
	)

	def __init__ (self, *args):
		super(myThread, self).__init__()
		for attribute, value in zip(self._attributes, args):
			setattr(self, attribute, value)

	def run(self):
		production_mode = self.PRODUCTION_MODE
		script_utility = self.script_utility
		google_ads_client = self.google_ads_client
		customer_id = self.customer_id
		account_name = self.account_name
		result_count = self.result_count
		errors = self.errors
		log_dict = self.log_dict

		google_ads_client_copy = copy.copy(google_ads_client)

		mutate_count = 0
		try:
			mutate_count = script_logic(
				production_mode, 
				google_ads_client_copy, 
				customer_id, 
				log_dict
			)
			result_count[customer_id] = True
			status = 'SUCCESS'

		except Exception as e:
			print(traceback.format_exc())
			status = 'FAILED | {}'.format(traceback.format_exc())
			errors[customer_id] = status

		script_utility.write_script_running_status(
			account_name, 
			customer_id, 
			status, 
			changes=mutate_count
		)

def script_logic(
	production_mode,
	google_ads_client, 
	customer_id, 
	log_dict
):

	if production_mode:
		google_ads_report = GoogleAdsReport(
			google_ads_client, 
			customer_id
		)

		ads_df = get_ad_performance_report(
			google_ads_report
		)

		label_name = 'Sample Label'
		ads_labels_merged_df = google_ads_report.get_labels_and_map_to_report(
			ads_df, 
			'ad', 
			label_name
		)

	mutate_count = 0
	log_dict[customer_id] = ['log data']

	return mutate_count


def get_ad_performance_report(
	google_ads_report
):

	fields = """customer.descriptive_name, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.status, ad_group_ad.labels, metrics.clicks, metrics.impressions, metrics.cost, metrics.conversion_value"""

	resource_name = """ad_group_ad"""

	conditions = """campaign.status = 'ENABLED' AND ad_group.status = 'ENABLED' AND ad_group_ad.status IN ('ENABLED', 'PAUSED') AND segments.date DURING LAST_30_DAYS"""

	query = google_ads_report.build_query(
		fields, 
		resource_name, 
		conditions=conditions
	)

	money_to_micros = ['metrics.cost']

	df = google_ads_report.download(
		query, 
		money_to_micros 
	)
	# returns roas column if cost and revenue added in fields as roas = True is default

	return df

def send_email(
	api_connector_factory, 
	log_dict, 
	receiver
):

	final_message_text =""

	subject = ''

	connector = api_connector_factory.get_connector(
		'GMAIL'
	)
	gmail_service = connector.connect()
	gmail = Gmail(
		gmail_service
	)

	for customer_id, log_data in log_dict.items():
		final_message_text+="<li>Customer ID = "+str(customer_id)+"<br></br>Log data = "+str(len(log_data))+"</li><br></br>"
	final_message_text+="</ul>"

	mail_content = {
		"to":f'{receiver}',
		"cc":'',
		"bcc":'',
		"subject":subject, 
		"message_text":final_message_text,
		"attachment_folder_path" :'', 
		"attachments": [],
		"html": True
	}
	gmail.send(
		mail_content
	)
