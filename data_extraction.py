import pandas as pd
import tabula
import requests
import boto3
import yaml
from botocore.exceptions import NoCredentialsError, ClientError

class DataExtractor:

    '''
    Data Extraction class extract the data from multiple sources including and not limited to:
    > APIs
    > AWS RDS
    > AWS s3 buckets in multple formats such as CSV,PDF and JSON
    > JSON

    Attributes:
    ----------
    header: dict
        a private dictionary variable containing the API key

    __store_number: string
        a private string containing the url for getting the total number of stores
    
    __store_url: string
        a private string containing the url for the store data

    __s3_bucket_name: string
        a private string containing the s3 bucket url

    __s3_file : string
        a private string containing the file name inside the s3 bucket

    __card_data: string
        a private string containing the url for getting the card_details data
    
    __dates_data: sting
         a private string containing the url for getting the data events data
    
    Methods:
    -------
    read_rds_table(table_name, engine)
        gets the data from the aws RDS database based on the table name
    
    retrieve_pdf_data()
        gets the data from the url pdf link. the pdf is in an AWS S3 bucket
    
    extract_from_s3()
        Get data from s3 bucket 

    list_number_of_stores()
        gets the list of stores from the url
    
    retrieve_stores_data(number_of_stores)
        retrieve each store data and store the results as a dataframe

    get_date_data()
        retrieve the date data from the url (the data is in a json format) and store the results in
        a dataframe
    '''

    def __init__(self):
        self.__header = self.read_yaml_file("creds/api_creds.yaml")
        self.__store_number = self.read_yaml_file("creds/api_creds.yaml")['retrieve_store_total']
        self.__store_url = self.read_yaml_file("creds/api_creds.yaml")['retrieve_store_data']
        self.__s3_bucket_name = self.read_yaml_file("creds/api_creds.yaml")['data_handling_s3']
        self.__s3_file = self.read_yaml_file("creds/api_creds.yaml")['file_s3']
        self.__card_data = self.read_yaml_file("creds/api_creds.yaml")['cards_details_data']
        self.__dates_data = self.read_yaml_file("creds/api_creds.yaml")['dates_data']

    def read_yaml_file(self, api_yaml):
        try:
            with open(api_yaml, 'r') as file:
                api_creds = yaml.safe_load(file)
                return api_creds
        except yaml.YAMLError as e:
                    print(f"Error reading YAML file: {e}")
                    return None

    def read_rds_table(self, table_name, engine):
         '''
         This function gets the data from AWS RDS based on the table name and store the results in
         a dataframe

        Parameters:
        -----------
         table_name: str
            the name of the table
         
    
         engine: sqlalchemy Engine
            sqlalchemy database engine

        
        Return:
        -------
            df: DataFrame
                the dataframe to be cleaned
         '''
         df = pd.read_sql_table(table_name, engine)
         return df
    
    def retrieve_pdf_data(self):
        '''
        This function gets the data from the url thats in a pdf format 
        and store the results as a pandas dataframe


        Paramters:
        ----------
            url: str
                the url

        Return:
        -------
            df: DataFrame
                the dataframe to be cleaned
        
        '''
        multiple_df = tabula.read_pdf(self.__card_data, pages='all')
        df = pd.concat(multiple_df)
        return df
        

    def extract_from_s3(self):
        '''
        This is a function that gets the data from aws s3 bucket,
        download the data as a csv and store result as a pandas dataframe.

        Return:
        ------
            df: DataFrame
                the dataframe to be cleaned
        '''
        try:
            s3 = boto3.client('s3')
            s3.download_file(self.__s3_bucket_name, self.__s3_file, './products.csv')
            df = pd.read_csv("./products.csv")
            return df

        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)


    def list_number_of_stores(self):
        '''
        This function get the number of stores from the url

        Parameter:
        ----------
            url: string
                the url link containing the json data containing the number of stores

        Return:
        -------
            number_of_stores: int
                number of stores

        '''
        try:
            number_of_stores = requests.get(self.__store_number, headers=self.__header)
            return number_of_stores.json()['number_stores']
        except requests.RequestException as e:
            print(f"Error: {e}")

    
    def retrieve_stores_data(self, number_of_stores):
        '''
        This function gets all the store data from the url and returns a dataframe.

        Parameter:
        ----------
            number_of_stores: int
                the max number of stores avaliable

        Return:
        ------
            df: DataFrame
                the dataframe to be cleaned

        '''
        list_of_stores = []
        for store in range(0,number_of_stores):
            url = f'{self.__store_url}{store}'
            new_response = requests.get(url, headers=self.__header)
            list_of_stores.append(new_response.json())
        df = pd.DataFrame(list_of_stores)
        return df
    
    def get_date_data(self):
        '''
        This function gets the data relating to dates from url and returns a dataframe 

        Parameter:
        ----------
            url: string
                the url link containing the json data containing the dates

        Return:
        ------
            df: DataFrame
                the dataframe to be cleaned
        '''
        try:
            date_event_data = requests.get(self.__dates_data)
            df = pd.DataFrame()
            for key, values in date_event_data.json().items():
                df_temp = pd.DataFrame.from_dict(values, orient='index', columns=[key])
                df = df_temp.join(df)
            df = df[df.columns[::-1]]
            return df 
        except requests.RequestException as e:
            print(f"Error: {e}")
        


if __name__ == "__main__":
    data_extractor = DataExtractor()
    # number_of_stores = data_extractor.list_number_of_stores()
    # print(number_of_stores)
    # df = data_extractor.retrieve_stores_data(number_of_stores)
    # print(df)
    # print(number_of_stores)
    #print(number_of_stores)
    #print(data_extractor.retrieve_stores_data())
    df = data_extractor.get_date_data()
    print(df)