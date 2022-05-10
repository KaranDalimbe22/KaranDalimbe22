"""This example updates a keyword for the specified ad group.

https://developers.google.com/google-ads/api/docs/client-libs/python/empty-message-fields?hl=vi
https://developers.google.com/google-ads/api/docs/client-libs/python/field-masks

python update_keywords.py -c 4861039205 -a 98878943903 -k 302272685926

"""
import argparse
import sys

from google.api_core import protobuf_helpers
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.util import ResourceName


def main(client, customer_id, ad_group_id, criterion_id):

    agc_service = client.get_service("AdGroupCriterionService", version="v6")

    ad_group_criterion_operation = client.get_type("AdGroupCriterionOperation", version="v6")

    ad_group_criterion = ad_group_criterion_operation.update

    ad_group_criterion.resource_name = agc_service.ad_group_criteria_path(
        customer_id, ResourceName.format_composite(ad_group_id, criterion_id)
        )

    ad_group_criterion.status = client.get_type(
        "AdGroupCriterionStatusEnum", version="v6"
    ).PAUSED
    # al_url = ad_group_criterion.final_urls.append("https://www.example.com")
    fm = protobuf_helpers.field_mask(None, ad_group_criterion)
    ad_group_criterion_operation.update_mask.CopyFrom(fm)

    try:
        agc_response = agc_service.mutate_ad_group_criteria(
            customer_id, [ad_group_criterion_operation]
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

    print("Updated keyword %s." % agc_response.results[0].resource_name)


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()
    parser = argparse.ArgumentParser(
        description=("Pauses an ad in the specified customer's ad group.")
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