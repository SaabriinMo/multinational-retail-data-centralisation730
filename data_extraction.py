import pandas as pd
import tabula
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class DataExtractor:

    def __init__(self):
        self.__header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX" }

    def read_rds_table(self, table_name, engine):
         df = pd.read_sql_table(table_name, engine)
         return df
    
    def retrieve_pdf_data(self, url):
        multiple_df = tabula.read_pdf(url, pages='all')
        df = pd.concat(multiple_df)
        return df
        

    def extract_from_s3(self):
        try:
            s3 = boto3.client('s3')
            s3.download_file('data-handling-public', 'products.csv', './products.csv')
            df = pd.read_csv("./products.csv")
            return df

        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)


    def list_number_of_stores(self, url):
        number_of_stores = requests.get(url, headers=self.__header)
        return number_of_stores.json()['number_stores']
    
    def get_date_data(self, url):
        date_event_data = requests.get(url)
        df = pd.DataFrame()
        for key, values in date_event_data.json().items():
            df_temp = pd.DataFrame.from_dict(values, orient='index', columns=[key])
            df = df_temp.join(df)
        df = df[df.columns[::-1]]
        return df


    def retrieve_stores_data(self, number_of_stores):
        list_of_stores = []
        for store in range(0,number_of_stores):
            url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}'
            new_response = requests.get(url, headers=self.__header)
            list_of_stores.append(new_response.json())
        df = pd.DataFrame(list_of_stores)
        return df
    
        


if __name__ == "__main__":
    data_extractor = DataExtractor()
    df = data_extractor.extract_from_s3()
    print(df)

    #print(data_extractor.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"))