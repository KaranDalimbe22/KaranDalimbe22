import os
import sys
import argparse

import google.ads.google_ads.client 
from google.ads.google_ads.client  import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException


def get_campaigns(client, customer_id):

	ga_service = client.get_service("GoogleAdsService", version="v6")

	query = """SELECT campaign.id, campaign.name 
		FROM campaign
		WHERE campaign.status = PAUSED"""

	# Issues a search request using streaming.
	response = ga_service.search_stream(customer_id, query=query)
	try:
		for batch in response:
			for row in batch.results:
				print(
					f"Campaign with ID {row.campaign.id} and name "
					f'"{row.campaign.name}" was found.'
				)
	except GoogleAdsException as ex:
		print(
			f'Request with ID "{ex.request_id}" failed with status '
			f'"{ex.error.code().name}" and includes the following errors:'
		)
		for error in ex.failure.errors:
			print(f'\tError with message "{error.message}".')
			if error.location:
				for field_path_element in error.location.field_path_elements:
					print(f"\t\tOn field: {field_path_element.field_name}")
		sys.exit(1)

if __name__ == '__main__':
	# GoogleAdsClient will read the google-ads.yaml configuration file in the
	# home directory if none is specified.
	google_ads_client = (
		google.ads.google_ads.client.GoogleAdsClient.load_from_storage()
	)

	parser = argparse.ArgumentParser(
		description=(
			"Sample reporting script for rows to dataframe."
		)
	)
	# The following argument(s) should be provided to run the example.
	parser.add_argument(
		"-c",
		"--customer_id",
		type=str,
		required=True,
		help="The Google Ads customer ID.",
	)
	args = parser.parse_args()

	# python get_campaigns.py -c 4861039205   ---testing customer id
	get_campaigns(google_ads_client, args.customer_id)

