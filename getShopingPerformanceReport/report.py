# Standard library imports
from dataclasses import dataclass

# Third party imports
import pandas as pd
import json_flatten
# local application imports

from genericModules.google_ads_operations import GoogleAdsReport


# Global variables


@dataclass
class Report:
    """" Report class """
    google_ads_client: object
    script_utility: object
    api_connector_factory: object
    merchant_center_api_connector_factory: object
    env_config: dict
    customer_id: str

    def __post_init__(self):
        self.google_ads_report: object = GoogleAdsReport(self.google_ads_client, self.customer_id)


    def get_data_for_last_month(
        self
    )-> pd.DataFrame:

        fields = """campaign.advertising_channel_type,segments.product_title, campaign.id,metrics.clicks, metrics.cost_micros,  metrics.conversions_value, metrics.impressions, campaign.shopping_setting.merchant_id, segments.product_item_id"""
        
        resource_name = """shopping_performance_view"""

        money_to_micros = ['metrics.cost_micros']
        
        conditions = f"""segments.date DURING LAST_MONTH """
        
        query = self.google_ads_report.build_query(
            fields, 
            resource_name, 
            conditions=conditions
        )

        df =self.google_ads_report.download(
            query, 
            money_to_micros, 
            include_roas=False
        )
        df=df.iloc[:, [0,6,1,2,3,4,5,7]]
               
        return df

    def get_product_from_merchant_center(
        self, 
        merchant_center_ids:list=[]
    )-> pd.DataFrame:
        """
        This function helps to get and genrate the Data Frame for the Merchant Account
        """

        merchant_vcenter_connector = self.merchant_center_api_connector_factory.get_connector('MERCHANT_CENTER')
        merchant_center_service = merchant_vcenter_connector.connect()
        products_list = []

        if merchant_center_ids:
            for merchant_center_id in merchant_center_ids:
                merchant_id = int(merchant_center_id)

                request = merchant_center_service.products().list(merchantId=merchant_id, maxResults=250)
                while request is not None:
                    results = request.execute()
                    if 'resources' in results:
                        products = results.get('resources')

                        for product in products:
                            flatten_data = json_flatten.flatten(product)
                            flatten_data['mCId'] = str(merchant_id)
                            products_list.append(flatten_data)
                    
                    request = merchant_center_service.products().list_next(request, results)
        
        products_df = pd.DataFrame(products_list)	

        if not products_df.empty:

            return products_df
        else:
            return pd.DataFrame()
    
