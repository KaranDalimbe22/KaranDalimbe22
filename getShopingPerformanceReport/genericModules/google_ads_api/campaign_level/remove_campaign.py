import os
import sys
import argparse

import google.ads.google_ads.client 
from google.ads.google_ads.client  import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

def remove_campaign(client, customer_id, campaign_id):


	campaign_service = client.get_service("CampaignService", version="v6")
	campaign_operation = client.get_type("CampaignOperation", version="v6")

	resource_name = campaign_service.campaign_path(customer_id, campaign_id)
	campaign_operation.remove = resource_name

	try:
		campaign_response = campaign_service.mutate_campaigns(
			customer_id, [campaign_operation]
		)
	except google.ads.google_ads.errors.GoogleAdsException as ex:
		print(
			'Request with ID "%s" failed with status "%s" and includes the '
			"following errors:" % (ex.request_id, ex.error.code().name)
		)
		for error in ex.failure.errors:
			print('\tError with message "%s".' % error.message)
			if error.location:
				for field_path_element in error.location.field_path_elements:
					print("\t\tOn field: %s" % field_path_element.field_name)
		sys.exit(1)

	print("Removed campaign %s." % campaign_response.results[0].resource_name)

if __name__ == '__main__':
	# GoogleAdsClient will read the google-ads.yaml configuration file in the
	# home directory if none is specified.
	google_ads_client = (
		google.ads.google_ads.client.GoogleAdsClient.load_from_storage()
	)

	parser = argparse.ArgumentParser(
		description=(
			"Removes given campaign for the specified customer."
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
	parser.add_argument(
		"-i", "--campaign_id", type=str, required=True, help="The campaign ID."
	)
	args = parser.parse_args()

	# testing_customer_id = 4861039205
	remove_campaign(google_ads_client, args.customer_id, args.campaign_id)
