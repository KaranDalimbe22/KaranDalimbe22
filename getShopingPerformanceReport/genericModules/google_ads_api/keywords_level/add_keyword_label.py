"""

https://developers.google.com/google-ads/api/docs/reporting/labels?hl=en#add_campaign_labels
https://developers.google.com/google-ads/api/docs/client-libs/python/resource-names
https://developers.google.com/google-ads/api/fields/v6/ad_group_criterion_label?hl=en

python add_keyword_label.py -c 4861039205 -a 98878943903 -k 880502679722 -l 12398113842
python add_keyword_label.py -c 4861039205 -a 98878943903 -k 439253784679 -l 12398113842

"""

import argparse
import sys

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.util import ResourceName

def main(client, customer_id, ad_group_id, keyword_id ,label_id):
    print(customer_id,label_id)

    label_service = client.get_service("LabelService", version="v6")

    ad_group_criterion_label_service = client.get_service(
        "AdGroupCriterionLabelService", version="v6"
    )
    # Build the resource name of the label to be added across the campaigns.
    label_resource_name = label_service.label_path(customer_id, label_id)

    operations = []

    ad_group_criterion_service = client.get_service('AdGroupCriterionService')

    composite_id = ResourceName.format_composite(ad_group_id, keyword_id)
    resource_name = ad_group_criterion_service.ad_group_criteria_path(customer_id, composite_id)

    ad_group_criterion_label_operation = client.get_type("AdGroupCriterionLabelOperation", version="v6")

    ad_group_criterion_label = ad_group_criterion_label_operation.create
    ad_group_criterion_label.ad_group_criterion = resource_name
    ad_group_criterion_label.label = label_resource_name
    operations.append(ad_group_criterion_label_operation)

    try:
        response = ad_group_criterion_label_service.mutate_ad_group_criterion_labels(
            customer_id, operations
        )
        print(f"Added {len(response.results)} keyword label:")
        for result in response.results:
            print(result.resource_name)
    except GoogleAdsException as error:
        print(
            'Request with ID "{}" failed with status "{}" and includes the '
            "following errors:".format(
                error.request_id, error.error.code().name
            )
        )
        for error in error.failure.errors:
            print('\tError with message "{}".'.format(error.message))
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(
                        "\t\tOn field: {}".format(field_path_element.field_name)
                    )
        sys.exit(1)

if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()
    parser = argparse.ArgumentParser(
        description=("")
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
        "-a",
        "--ad_group_id",
        type=str,
        required=True,
        help="The adgroup id",
    )
    parser.add_argument(
        "-k",
        "--keyword_id",
        type=str,
        required=True,
        help="The keyword id",
    )
    parser.add_argument(
        "-l",
        "--label_id",
        type=str,
        required=True,
        help="The label id",
    )
    args = parser.parse_args()

    main(
        google_ads_client, args.customer_id, args.ad_group_id, args.keyword_id, args.label_id
    )