
"""This example gets non-removed responsive search ads in a specified ad group.
To add responsive search ads, run basic_operations/add_responsive_search_ad.py.
To get ad groups, run basic_operations/get_ad_groups.py.

Ad group level = python get_rsa_ad.py -c customer_id -a adgroup_id
campaign level = python get_rsa_ad.py -c customer_id -campaign campaign_id
account Level = python get_rsa_ad.py -c customer_id 

"""

import argparse
import sys

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException


_DEFAULT_PAGE_SIZE = 1000


def main(client, customer_id, page_size, args):
   
    

    ga_service = client.get_service("GoogleAdsService", version="v6")

    query = """
        SELECT
          ad_group.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.responsive_search_ad.headlines,
          ad_group_ad.ad.responsive_search_ad.descriptions,
          ad_group_ad.status
        FROM ad_group_ad
        WHERE
          ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
          AND ad_group_ad.status != 'REMOVED'"""

    # Optional: Specify an ad group ID to restrict search to only a given
    # ad group
    # if args.account_id is not None:
    #     query = query + f" AND account.id = {args.account_id}" 
    
    if args.campaign_id is not None:
        query = query + f" AND campaign.id = {args.campaign_id}"

    elif args.ad_group_id:
        query = query + f" AND ad_group.id = {args.ad_group_id}"
    else:
        query = query    

    print("Query:",query)

    results = ga_service.search(customer_id, query=query, page_size=page_size)
    aga_status_enum = client.get_type(
        "AdGroupAdStatusEnum", version="v6"
    ).AdGroupAdStatus

    print("total RSA ads:=",len(list(results)))
    try:
        one_found = False

        for row in results:
            one_found = True
            ad = row.ad_group_ad.ad
            print(
                f"Responsive search ad with resource name "
                f'"{ad.resource_name}", '
                f"status {aga_status_enum.Name(row.ad_group_ad.status)} "
                f"was found."
            )
            print(
                "Headlines:\n{}\nDescriptions:\n{}\n".format(
                    "\n".join(
                        _ad_text_assets_to_strs(
                            client, ad.responsive_search_ad.headlines
                        )
                    ),
                    "\n".join(
                        _ad_text_assets_to_strs(
                            client, ad.responsive_search_ad.descriptions
                        )
                    ),
                )
            )

        if not one_found:
            print("No responsive search ads were found.")

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


def _ad_text_assets_to_strs(client, assets):
    """Converts a list of AdTextAssets to a list of user-friendly strings."""
    sa_field_type_enum = client.get_type(
        "ServedAssetFieldTypeEnum", version="v6"
    ).ServedAssetFieldType
    s = []
    for asset in assets:
        s.append(
            '\t"'
            + asset.text
            + '" pinned to '
            + sa_field_type_enum.Name(asset.pinned_field)
        )
    return s


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()

    parser = argparse.ArgumentParser(
        description="List responsive display ads for specified customer. "
        "An ad_group is optional."
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
        required=False,
        help="The ad group ID. ",
    )

    parser.add_argument(
        "-campaign",
        "--campaign_id",
        type=str,
        required=False,
        help="The campaign ID. ",
    )

    args = parser.parse_args()

    main(
        google_ads_client,
        args.customer_id,
        _DEFAULT_PAGE_SIZE,
        args
    )


