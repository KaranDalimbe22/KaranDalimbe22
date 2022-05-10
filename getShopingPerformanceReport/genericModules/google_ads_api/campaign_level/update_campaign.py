import os
import sys
import argparse

import google.ads.google_ads.client 
from google.ads.google_ads.client  import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.api_core import protobuf_helpers

def update_campiagn(client, customer_id, campaign_id):

	campaign_service = client.get_service("CampaignService", version="v6")
	# Create campaign operation.
	campaign_operation = client.get_type("CampaignOperation", version="v6")
	campaign = campaign_operation.update
	campaign.resource_name = campaign_service.campaign_path(
		customer_id, campaign_id
	)
	campaign.status = client.get_type("CampaignStatusEnum", version="v6").PAUSED
	campaign.network_settings.target_search_network = False
	# Retrieve a FieldMask for the fields configured in the campaign.
	fm = protobuf_helpers.field_mask(None, campaign)
	print(fm)
	campaign_operation.update_mask.CopyFrom(fm)
	print(campaign_operation)

	# Update the campaign.
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

	print("Updated campaign %s." % campaign_response.results[0].resource_name)

if __name__ == '__main__':
	# GoogleAdsClient will read the google-ads.yaml configuration file in the
	# home directory if none is specified.
	google_ads_client = (
		google.ads.google_ads.client.GoogleAdsClient.load_from_storage()
	)

	parser = argparse.ArgumentParser(
		description=(
			"Updates given campaign for the specified customer."
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
	update_campiagn(google_ads_client, args.customer_id, args.campaign_id)

