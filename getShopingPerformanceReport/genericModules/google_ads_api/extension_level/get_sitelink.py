#!/usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Fetch sitelinks.

python get_sitelink.py -c 4861039205 -t SITELINK
"""


import argparse
import datetime
import os
import sys
import uuid
from collections import namedtuple
import json_flatten
import json
import pandas as pd

from google.protobuf import json_format
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException


_DateRange = namedtuple("_DateRange", ["start_datetime", "end_datetime"])
_date_format = "%Y-%m-%d %H:%M:%S"


# [START add_sitelinks_1]
def main(client, customer_id, extension_type):
    ga_service = client.get_service("GoogleAdsService", version="v6")

    query = """
                SELECT
                    extension_feed_item.resource_name,
                    extension_feed_item.sitelink_feed_item.link_text,
                    extension_feed_item.sitelink_feed_item.final_urls
                FROM
                    extension_feed_item
                WHERE
                    extension_feed_item.sitelink_feed_item.link_text IS NOT NULL 

                """

    # Issues a search request using streaming.
    response = ga_service.search_stream(customer_id, query)

    list_data = []
    try:
        for batch in response:
            for row in batch.results:
                json_str = json_format.MessageToJson(row)
                d = json.loads(json_str)
                list_data.append(json_flatten.flatten(d))
        df = pd.DataFrame(list_data)
        print(df)
        # df.to_excel(os.path.join(os.getcwd(), 'Extensions_test_account.xlsx'), index=False)

    try:
        customer_extension_setting =  client.get_service("CustomerExtensionSettingService", version="v6")
        
        resource_name = customer_extension_setting.customer_extension_setting_path(customer_id, extension_type)
        
        response = customer_extension_setting.get_customer_extension_setting(resource_name)
        print(response)

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


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()

    parser = argparse.ArgumentParser(
        description=("Retrieves extensions.")
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
        "-t",
        "--type_of_extension",
        type=str,
        required=True,
        help="The Google Ads customer ID.",
    )
    args = parser.parse_args()

    main(google_ads_client, args.customer_id, args.type_of_extension)
