import os
import sys
import pandas as pd
import json
from collections import ChainMap
from google.protobuf import json_format
import time
import argparse

import google.ads.google_ads.client 
from google.ads.google_ads.client  import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

def testing(client, customer_id):

	"""sample customer performance"""
	customer_service = client.get_service("CustomerService", version="v6")
	# resource_name = customer_service.customer_path(customer_id)
	# response = customer_service.get_customer(resource_name=resource_name)

	# ad_id = '436066512364'
	# ad_service = client.get_service('AdService')
	# resource_name = ad_service.ad_path(customer_id, ad_id)
	# response = ad_service.get_ad(resource_name=resource_name)

	# df = proto_resource_to_df_conversion(response)
	# print(df)
	# print(df)

	# end_time = time.time() - start_time
	# print('end_time = ',end_time)

	# df.to_excel('test_df.xlsx',index=None)

	##################################################################

	"""Using GoogleAdsService Stream Response"""

	start_time = time.time()
	ga_service = client.get_service("GoogleAdsService", version="v6")

	"""sample campaign prformance"""
	# query = """SELECT campaign.id, campaign.name, campaign.status, metrics.clicks, metrics.impressions
	# 	FROM campaign"""

	"""sample adgroup performance"""
	# query = """SELECT campaign.id, campaign.name, campaign.status, ad_group.id,ad_group.name, ad_group.status, metrics.clicks, metrics.impressions FROM ad_group"""

	"""sample keyword performance"""
	# query = """SELECT campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,ad_group_criterion.keyword.match_type, ad_group_criterion.status, metrics.clicks, metrics.impressions
	# 	FROM keyword_view LIMIT 3"""

	"""sample ad performance"""
	query = """
		SELECT
		  ad_group.id,
		  ad_group_ad.ad.id,
		  ad_group_ad.ad.expanded_text_ad.headline_part1,
		  ad_group_ad.ad.expanded_text_ad.headline_part2,
		  ad_group_ad.status, metrics.clicks, metrics.impressions
		FROM ad_group_ad
		WHERE ad_group_ad.ad.type = EXPANDED_TEXT_AD AND ad_group_ad.ad.id = '436066512364'"""

	"""sample audience performance"""
	# query = """
	# SELECT 
	# ad_group_audience_view.resource_name,
	# segments.ad_network_type,
	# metrics.clicks, metrics.impressions
	# FROM ad_group_audience_view LIMIT 4
	# """

	"""sample search term performance"""
	# query = """
	# SELECT 
	# search_term_view.ad_group,
	# search_term_view.resource_name,
	# search_term_view.search_term,
	# search_term_view.status,
	# metrics.clicks, metrics.impressions
	# FROM search_term_view LIMIT 4
	# """

	"""sample shopping performance"""
	# query = """
	# SELECT 
	# shopping_performance_view.resource_name,
	# segments.product_brand,
	# segments.product_title,
	# segments.product_bidding_category_level1,
	# segments.product_bidding_category_level2, 
	# segments.product_bidding_category_level3,
	# segments.product_item_id,
	# segments.product_merchant_id
	# FROM shopping_performance_view LIMIT 4
	# """

	response = ga_service.search_stream(customer_id, query=query)
	df = search_stream_to_df_conversion(response)

	print(df)

	end_time = time.time() - start_time
	print('end_time = ',end_time)

	df.to_excel('test_df.xlsx',index=None)

def search_stream_to_df_conversion(response:object):

	result_list = []
	for batch in response:
		for row in batch.results:
			row_to_dict = json_format.MessageToDict(row)
			googleads_row_list = []
			for key in row_to_dict.copy().keys():
				for key1 in row_to_dict[key].copy().keys():
					row_to_dict[key][".".join([key,key1])] = row_to_dict[key].pop(key1)
					if isinstance(row_to_dict[key][".".join([key,key1])],dict):
						for key2 in row_to_dict[key][".".join([key,key1])].copy().keys():
							row_to_dict[key][".".join([key,key1])][".".join([key1,key2])] = row_to_dict[key][".".join([key,key1])].pop(key2)

							if isinstance(row_to_dict[key][".".join([key,key1])][".".join([key1,key2])],dict):
								for key3 in row_to_dict[key][".".join([key,key1])][".".join([key1,key2])].copy().keys():
									row_to_dict[key][".".join([key,key1])][".".join([key1,key2])][".".join([key2,key3])] = row_to_dict[key][".".join([key,key1])][".".join([key1,key2])].pop(key3)

								googleads_row_list.append(row_to_dict[key][".".join([key,key1])][".".join([key1,key2])])
								del row_to_dict[key][".".join([key,key1])][".".join([key1,key2])]

						googleads_row_list.append(row_to_dict[key][".".join([key,key1])])
						del row_to_dict[key][".".join([key,key1])]

				googleads_row_list.append(row_to_dict[key])

			merged_dict = dict(ChainMap(*googleads_row_list))
			result_list.append(merged_dict)

	df = pd.DataFrame(result_list)
	df = df.fillna('--')

def proto_resource_to_df_conversion(response:object):
	"""converts any respone type to dataframe"""

	row_to_dict = json_format.MessageToDict(response)
	googleads_row_list = []
	for key in row_to_dict.copy().keys():
		if isinstance(row_to_dict[key],dict):
			for key1 in row_to_dict[key].copy().keys():
				row_to_dict[key][".".join([key,key1])] = row_to_dict[key].pop(key1)
				if isinstance(row_to_dict[key][".".join([key,key1])],dict):
					for key2 in row_to_dict[key][".".join([key,key1])].copy().keys():
						row_to_dict[key][".".join([key,key1])][".".join([key1,key2])] = row_to_dict[key][".".join([key,key1])].pop(key2)

					if isinstance(row_to_dict[key][".".join([key,key1])][".".join([key1,key2])],dict):
						for key3 in row_to_dict[key][".".join([key,key1])][".".join([key1,key2])].copy().keys():
							row_to_dict[key][".".join([key,key1])][".".join([key1,key2])][".".join([key2,key3])] = row_to_dict[key][".".join([key,key1])][".".join([key1,key2])].pop(key3)

						googleads_row_list.append(row_to_dict[key][".".join([key,key1])][".".join([key1,key2])])
						del row_to_dict[key][".".join([key,key1])][".".join([key1,key2])]

			googleads_row_list.append(row_to_dict[key])
			del row_to_dict[key]

	googleads_row_list.append(row_to_dict)

	# googleads_row_list = dict(ChainMap(*googleads_row_list))

	df = pd.DataFrame(googleads_row_list)
	df = df.fillna('--')

	return df


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

	# customer_id = 8858752180

	testing(google_ads_client, args.customer_id)

