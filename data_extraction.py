import pandas as pd
import tabula
import requests
import boto3
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
    Methods:
    -------
    read_rds_table(table_name, engine)
        gets the data from the aws RDS database based on the table name
    
    retrieve_pdf_data(url)
        gets the data from the url pdf link. the pdf is in an AWS S3 bucket
    
    extract_from_s3()
        Get data from s3 bucket 

    list_number_of_stores(url)
        gets the list of stores from the url
    
    retrieve_stores_data(number_of_stores)
        retrieve each store data and store the results as a dataframe

    get_date_data(url)
        retrieve the date data from the url (the data is in a json format) and store the results in
        a dataframe
    '''

    def __init__(self):
        self.__header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX" }

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
    
    def retrieve_pdf_data(self, url):
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
        multiple_df = tabula.read_pdf(url, pages='all')
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
        number_of_stores = requests.get(url, headers=self.__header)
        return number_of_stores.json()['number_stores']
    
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
            url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}'
            new_response = requests.get(url, headers=self.__header)
            list_of_stores.append(new_response.json())
        df = pd.DataFrame(list_of_stores)
        return df
    
    def get_date_data(self, url):
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
        date_event_data = requests.get(url)
        df = pd.DataFrame()
        for key, values in date_event_data.json().items():
            df_temp = pd.DataFrame.from_dict(values, orient='index', columns=[key])
            df = df_temp.join(df)
        df = df[df.columns[::-1]]
        return df
    
        


if __name__ == "__main__":
    data_extractor = DataExtractor()
    df = data_extractor.extract_from_s3()
    print(df)

    #print(data_extractor.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"))