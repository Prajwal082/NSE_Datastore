# Import the Session class from the snowflake.snowpark.session module
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *

import pandas as pd
import requests,pprint
from datetime import  date,timedelta


class Nse:

    def __init__(self) -> None:
        self.__headers = {'User-Agent' :'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}
        self.__session = requests.session()
        self.__session.get("https://www.nseindia.com",headers = self.__headers)

        self.__snow_session = self.create_session_object()
        self.read_PramFile()

        old_date = date.today() - timedelta(days=90)
        self.old_date = old_date.strftime("%d-%m-%Y")
        self.today_date = date.today().strftime('%d-%m-%Y')

    # Create Session object
    def create_session_object(self):
        # Define the connection parameters
        connection_parameters = {
          "account": "SIINKBW-LC23685",
          "user": "steve",
          "password": "Prajwal082",
          "role": "SYSADMIN",
          "warehouse": "NSE_DWH",
          "database": "NSE",
          "schema": "STOCK"
        }
        
        
        # Create the session object with the provided connection parameters
        session = Session.builder.configs(connection_parameters).create()
        
        # Print the contents of your session object
        # Print(session)
        
        return session 

    def read_PramFile(self) -> dict:

        sheet_name = "1yoQZNPwdYoQte13GKAsEzQ4WkABx77usQiSW2O-Hj-I"
        df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_name}/export?format=csv")

        self.dict_conxt = {}

        for script, is_Active in zip(list(df['Script_Name']),list(df['Is_Active'])) : 
            self.dict_conxt[f'{script}'] = is_Active

        return self.dict_conxt 

    def get_DeliveryData(self):

        for key, val in self.dict_conxt.items():
            
            if val==1:
                print(f"Fetching data for {key}")
                
                URL=f"https://www.nseindia.com/api/historical/securityArchives?from={self.old_date}&to={self.today_date}&symbol={key}&dataType=priceVolumeDeliverable&series=ALL"

                response = self.__session.get(URL, headers = self.__headers)

                if response.status_code == 200:
                    print("Request Acceped..!")

                    stock_df = pd.DataFrame(response.json()["data"])
                    stock_df = stock_df.drop(columns=['_id','CH_SERIES','CH_MARKET_TYPE','CH_ISIN','TIMESTAMP','createdAt','updatedAt','__v','VWAP','mTIMESTAMP'])

                    if 'CA' in stock_df.columns:
                        stock_df = stock_df.set_index('CA',drop=True)

                    df_snow = self.__snow_session.create_dataframe(stock_df) 

                    df_snow.create_or_replace_temp_view("source_data")

                    target = self.__snow_session.table("NSE.STOCK.DELIVERY_DATA")

                    # df_snow = df_snow.select(when(col('CH_TIMESTAMP') == '2024-02-16',100.99).alias("CH_TRADE_HIGH_PRICE"),df_snow.columns[-1:])
                    # df_snow.write.mode("overwrite").save_as_table("NSE.STOCK.DELIVERY_DATA", table_type="transient")

                    name = response.json()["data"][0]['CH_SYMBOL']

                    col_map = {cols: df_snow[cols] for cols in df_snow.columns}

                    # print(col_map)

                    target.merge( df_snow, 
                                 (target["CH_SYMBOL"] == df_snow["CH_SYMBOL"]) & (target["CH_TIMESTAMP"] == df_snow["CH_TIMESTAMP"]),
                                    [
                                        when_matched().update(col_map), 
                                        when_not_matched().insert(col_map)
                                    ]
                                )

                    print(f"Records Upserted for: {name}")

                else:
                    print(f"Response returned a status code {response.status_code}")

                    raise ConnectionRefusedError
                

if __name__ == '__main__':

    nse = Nse()

    nse.get_DeliveryData()

