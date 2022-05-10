# Standard library imports

import threading
import traceback
from dataclasses import dataclass

# Third party imports
import pandas as pd

# local application imports
from genericModules.spreadsheet import Spreadsheet
import report

# Global variables
GOOGLE_ADS_API_VERSION = 'v10'
MAX_PAGE_SIZE = 250


@dataclass
class Logic:
    PRODUCTION_MODE: bool
    script_utility: object
    customers_df: pd.DataFrame
    google_api_connector_factory: object
    google_ads_api_connector:object
    merchant_center_api_connector_factory:object
    result_count: int
    errors: dict
    env_config: dict
    url:object


    def main(self):
        self.call_threads()
        log_dict = {}
        if log_dict:
            if self.PRODUCTION_MODE:
                receiver = ''
            else:
                receiver = ''

        return self.result_count, self.errors

    def call_threads(self):
        threads = []
        for index, row in self.customers_df.iterrows():
            customer_id = row['Customer ID']
            account_name = row['Account Name']

            print('\nSCRIPT RUNNING FOR - ', str(customer_id))
            thread = threading.Thread(target=self.run_thread, args=[customer_id, account_name])
            thread.start()
            threads.append(
                thread
            )

        for thread in threads:
            thread.join()

    def run_thread(self, customer_id, account_name):
        mutate_count = 0
        try:
            mutate_count = self.script_logic(
                customer_id,
                account_name
            )
            self.result_count[customer_id] = True
            status = 'SUCCESS'

        except Exception as e:
            print(traceback.format_exc())
            status = 'FAILED | {}'.format(traceback.format_exc())
            self.errors[customer_id] = status

        self.script_utility.write_script_running_status(
            account_name,
            customer_id,
            status,
            changes=mutate_count
        )


    def script_logic(self, customer_id,account_name):
        print(f"Getting data for {account_name} account..")
        
        if self.PRODUCTION_MODE:
            google_ads_client= self.google_ads_api_connector.connect()

            report_utility = report.Report(
                google_ads_client,
                self.script_utility,
                self.google_api_connector_factory,
                self.merchant_center_api_connector_factory,
                self.env_config,
                customer_id
            )
            
            
            df=report_utility.get_data_for_last_month()
            
            merchant_center_ids=df['campaign.shopping_setting.merchant_id'].values.tolist()
            merchant_center_ids = list(set(merchant_center_ids))
            merchant_center_products_df = report_utility.get_product_from_merchant_center(merchant_center_ids)

            merchant_center_products_df = post_process_dataframe(merchant_center_products_df)


            final_products_df=df.merge(merchant_center_products_df, how='left', left_on=['campaign.shopping_setting.merchant_id', 'segments.product_item_id'], right_on=['mCId','itemId'])
            final_products_df = final_products_df[['campaign.advertising_channel_type','metrics.impressions','campaign.shopping_setting.merchant_id','campaign.id','metrics.clicks','metrics.conversions_value','metrics.cost_micros','imageLink','link','availability','itemId']]
            
            final_products_df = final_products_df.fillna(final_products_df.dtypes.replace({'int64': 0, 'object': '--', 'float64': 0.0}))
            final_products_df = final_products_df.rename(columns={'campaign.advertising_channel_type':'Campaign Type',
                                                                'metrics.impressions':'Impressions',
                                                                'campaign.shopping_setting.merchant_id':'Merchant Center Id',
                                                                'campaign.id':'Campaign Id',
                                                                'metrics.clicks':'Clicks',
                                                                'metrics.conversions_value':'Revenue',
                                                                'metrics.cost_micros':'Spent',
                                                                'imageLink':'Image Link',
                                                                'link':'Product Link',
                                                                'availability':'Status',
                                                                'itemId':'Product Id',
                                                                })
            final_products_df= final_products_df[['Merchant Center Id','Campaign Id','Campaign Type','Product Id','Product Link','Image Link','Clicks','Impressions','Spent','Revenue','Status']]
            val = final_products_df.values.tolist()
            col = final_products_df.columns.tolist()

            final_products_df.sort_values(by=['Revenue'], inplace=True, ascending=False)
            df1=final_products_df.head(50)
            val1 = df1.values.tolist()
            col1 = df1.columns.tolist()
   
            sort=final_products_df.sort_values(by=['Spent'],ascending=False)
            sort.sort_values(by=['Revenue'], inplace=True)
            cost_sort_df=sort.head(50)
            val2 = cost_sort_df.values.tolist()
            col2 = cost_sort_df.columns.tolist()

            out_stock_df = final_products_df[(final_products_df['Status']=='out of stock')]
            top_revenue_df = out_stock_df.sort_values(by=['Revenue'], ascending=False)
            val3 = top_revenue_df.values.tolist()
            col3 = top_revenue_df.columns.tolist()
            
            spreadsheet_connector = self.google_api_connector_factory.get_connector(
                'SPREADSHEET'
            )
            spreadsheet_service = spreadsheet_connector.connect()

            url=self.url
                    
            spreadsheet_obj = Spreadsheet(
                spreadsheet_service,
                url
                )
            
            spreadsheet_data= {
                'Merchant Data': {
                    
                    'A1': [col],
                    'A2':val
                },
                'Sort Data Top 50': {
                    
                    'A1': [col1],
                    'A2':val1
                },
                'Sort Data Worst 50': {
                    
                    'A1': [col2],
                    'A2':val2
                },
                'Out of Stock Data': {
                    
                    'A1': [col3],
                    'A2':val3
                },
            }
            spreadsheet_obj.write(spreadsheet_data)
            
        mutate_count = 0
        return mutate_count
        
        
    
@dataclass
class LogicPreProcessHelpers:
    script_utility: object
    google_ads_client: object
    google_api_connector_factory:object
    customer_id: str

    def get_percent_change(self,now,prev):

        if (now != 0 and prev ==0):
            return 1
        elif (now == 0 and prev !=0):
            return -1
        elif(now == 0 and prev == 0):
            return 0
        else:
            return ((now-prev)/prev)

def post_process_dataframe(df: pd.DataFrame):

    df['itemId'] = df['offerId'].str.lower()

    for column in df.columns:
        if 'metrics' in column:
            value = 0
        else:
            value = '--'

    df[column] = df[column].fillna(value)
    
    return df