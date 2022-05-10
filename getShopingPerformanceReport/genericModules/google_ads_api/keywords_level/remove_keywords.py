"""This example removes an existing keyword from an ad group.


Campaign name : Adhelp | Campaign Bewakoof
Adgroup name : 1 Plus 6 Back Cover

python remove_keywords.py -c 4861039205 -a 98878943903 -k 302272685926

"""


import argparse
import sys

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.util import ResourceName


def main(client, customer_id, ad_group_id, criterion_id):
    agc_service = client.get_service("AdGroupCriterionService", version="v6")
    agc_operation = client.get_type("AdGroupCriterionOperation", version="v6")

    resource_name = agc_service.ad_group_criteria_path(
        customer_id, ResourceName.format_composite(ad_group_id, criterion_id)
    )
    agc_operation.remove = resource_name

    try:
        agc_response = agc_service.mutate_ad_group_criteria(
            customer_id, [agc_operation]
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

    print(f"Removed keyword {agc_response.results[0].resource_name}.")


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()
    parser = argparse.ArgumentParser(
        description=("Removes given campaign for the specified customer.")
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
        "-a", "--ad_group_id", type=str, required=True, help="The ad group ID."
    )
    parser.add_argument(
        "-k",
        "--criterion_id",
        type=str,
        required=True,
        help="The criterion ID, or keyword ID.",
    )
    args = parser.parse_args()

    main(
        google_ads_client, args.customer_id, args.ad_group_id, args.criterion_id
    )