import numpy as np
import pandas as pd
import re

class DataCleaning:
    '''
    A Data Cleaning class that cleans the dataframe and returns a cleaned dataframe.

    Methods:
    -------
    clean_user_data(df)
        Cleans the legacy_user table from the AWS RDS 
    
    clean_orders_data(df)
        Cleans the orders_table table from the AWS RDS

    called_clean_store_data(df)
        Cleans the store data from API

    clean_card_data(df):
        Clean card data from a pdf from AWS s3 bucket

    clean_products_data(df):
         Clean card data from AWS s3 bucket

    clean_event_date(df):
        Clean date event data from url (AWS s3 bucket)
    
    clean_address(df)
        Cleans the address column from a DataFrame

    clean_longitude(df)
         Cleans the address column from a DataFrame

    clean_store_type(df)
        Cleans the store type column from a DataFrame

    clean_contient(df):
        Cleans the contient column from a DataFrame

    clean_country_code(df)
        Cleans the country code column from a DataFrame

    clean_email(df)
        Cleans the email column from a DataFrame

    clean_card_number(df)
        Cleans the card number column from a DataFrame
    
    clean_phone_number(regex, df)
        Cleans the phone number column from a DataFrame

    clean_non_numerical_data(df, column_name):
        Clean the column of all digits for charcter only stringcolumns

    clean_weight(df)
        Clean the weight column

    clean_month(df)
        Clean the month column

    clean_year(df)
        Clean the year column

    clean_day(df)
        Clean the day column

    

    HELPER FUNCTION

    remove_char_from_digit(df, column_name)
        Remove character from a string of digits from a column. 
        Assuming that the digits datatype is strings

    phone_number_plus(regex, phone_number_string)
        Checks if the first 2/3 numbers are either +1, +44(0) or +49(0), ammends the string
        and return the new number


    clear_null(df, column_name)
        Changes the string such as 'NULL' and 'N/A' to NaN Values 

    replace_string(df, column_name, string_from, string_to)
        replaces a string with another

    check_credit_card_length(card_number)
        Check the card number length and return either NaN or the card number if the 
        value meets the condition.
    
    weight_validator(weight_string)
        Check if the weight column follows a similar format
    
    convert_to_grams(metric_string)
        Converts the weight metrics to kg

    check_sum_digit(string)
        Check if the number of digit in a string exceed one
        For only letter/character string
    
    isyear(year_string)
        Check if the year(string) has 4 digits
    
    ismonth(month_string)
        Check if the month number(string) is between 1-12
    
    isdate(day_string)
        Check if the day number(string) is between 1-31

    '''
    def __init__(self):
        pass

    def clean_user_data(self, df):
        '''
        This function cleans the legacy_user Dataframe which was imported from AWS RDS. 
        It goes through each column and remove NaN Values, erronous data and etc.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   

        Returns:
        --------
            df: DataFrame
                the cleaned dataframe 
        
        '''
        self.clean_invalid_date(df, 'date_of_birth')
        self.clean_email(df)
        self.clean_address(df)
        self.clean_invalid_date(df, 'join_date')
        self.replace_string(df, 'country_code', 'GG', 'G')
        self.clean_phone_number(df, r'\+44\(0\)|\+1|\+49\(0\)')
        self.clear_null(df, 'first_name')
        self.clear_null(df, 'country')
        self.clear_null(df, 'country_code')
        df.dropna(inplace=True)
        return df
    
    def clean_card_data(self, df):
        '''
        This function cleans the card data Dataframe imported a pdf from AWS s3 bucket
        It goes through each column and remove NaN Values, erronous data and etc.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   

        Returns:
        --------
            df: DataFrame
                the cleaned dataframe 
        
        '''
        self.clean_card_number(df)
        self.clean_invalid_date(df, 'date_payment_confirmed')
        df.dropna(inplace=True)
        return df


    def clean_products_data(self, df):
        '''
        This function cleans the products_data Dataframe which was imported AWS s3 bucket
        It goes through each column and remove NaN Values, erronous data and etc.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   

        Returns:
        --------
            df: DataFrame
                the cleaned dataframe 
        
        '''

        self.clean_non_numerical_data(df, 'category')
        self.replace_string(df, 'product_price', '£', '')
        self.clean_non_numerical_data(df, 'removed')
        self.clean_invalid_date(df, 'date_added')
        df.dropna(inplace=True)
        self.clean_weight(df)
        df.drop(columns=['Unnamed: 0'], inplace=True)
        df.dropna(inplace=True)

        return df

    def clean_orders_data(self, df):
        '''
        This function cleans the orders table Dataframe which was imported from AWS RDS. 
        This drops speificed columns containing NaN values

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned 

        Returns:
        --------
             df: DataFrame
                the cleaned dataframe  
        
        '''
        df.drop(columns=["level_0", "first_name", "last_name", '1'],axis=1, inplace=True)
        return df


    def called_clean_store_data(self, df):
        '''
        This function cleans the store data Dataframe which was imported from API. 
        It goes through each column and remove NaN Values, erronous data and etc.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   

        Returns:
        --------
             df: DataFrame
                the cleaned dataframe
        
        '''
        df.drop('lat', axis=1, inplace=True)
        self.clean_address(df)
        self.clean_longitude(df)
        self.clean_non_numerical_data(df, 'locality')
        self.clean_latitude(df)
        self.clear_null(df, 'store_code')
        self.clean_staff_number(df)
        self.clear_null(df, 'staff_numbers')
        self.clean_invalid_date(df, 'opening_date')
        self.clean_store_type(df)
        self.clean_contient(df)
        self.clean_country_code(df)
        df.dropna(inplace=True)
        return df
    
    def clean_event_date(self, df):
        '''
        '''
        self.clean_invalid_date(df, 'timestamp')
        self.clean_year(df)
        self.clean_month(df)
        self.clean_day(df)
        df.dropna(inplace=True)
        return df


    def clean_address(self, df):
        '''
        This function cleans the address column by removing '\n' and replacing it with a space

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned 

        Returns:
        --------
             df: DataFrame
                the cleaned dataframe   
        
        '''
        df['address'] = df['address'].str.replace('\n', ' ')

    def clean_longitude(self,df):
        '''
        This function cleans the longitude column and checks if it follows 
        the longitude patterm, if not the specified row is set to NaN.
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned    
        '''
        
        pattern = r"^(\+|-)?(?:180(?:(?:\.0{1,6})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,6})?))$"
        invalid_longitude_mask = ~df['longitude'].astype(str).str.match(pattern)
        df.loc[invalid_longitude_mask, 'longitude'] = np.nan

    def clean_staff_number(self, df):
        '''
        This function cleans the staff column and remove any characters from the number
        (Assuming that the staff_number datatype is a string. )

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   
        '''
        df = self.remove_char_from_digit(df, 'staff_numbers')

    def clean_invalid_date(self, df, column_name):
        '''
        This function cleans any column containing datetime datatypes. Anything that's incompatiable to the format
        is set to NaN values.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned   
        '''
        df[column_name] = df[column_name].apply(pd.to_datetime,
                                                infer_datetime_format=True,
                                                errors='coerce')
    def clean_latitude(self, df):
        '''
        This function cleans the latitude column and checks if it follows 
        the longitude patterm, if not the specified row is set to NaN.
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned    
        '''
        pattern = r"^(\+|-)?(?:90(?:(?:\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,6})?))$"
        invalid_latitude_mask = ~df['latitude'].astype(str).str.match(pattern)
        df.loc[invalid_latitude_mask, 'latitude'] = np.nan

    def clean_store_type(self, df):
        '''
        This function cleans the store type column and checks if contains a mixture of
        numbers and characters. If so, the specified row is set to NaN. This function also 
        remove 'NULL' or 'NaN' type of string.
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned    
        '''
        regex = r'[A-Z0-9]{10}$'
        df['store_type'] = df['store_type'].astype(str)
        df.loc[df['store_type'].str.match(regex), 'store_type'] = np.nan
        self.clear_null(df, 'store_type')

    def clean_contient(self, df):
        '''
        This function cleans the contient column by removing 'ee' and 
        replacing it with nothing

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned    
        
        '''
        df['continent'] = df['continent'].str.replace("ee", "")

    def clean_country_code(self, df):
        '''
        This function cleans the contient column by removing 'GG' and 
        replacing it with 'G'

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned    
        
        '''
        df['country_code'] = df['country_code'].str.replace("GG", "G")

    def clean_email(self, df):
        '''
        This function cleans the contient column by removing '@@' and 
        replacing it with '@'. It then checks if follows the email format.

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned  

        Returns:
            df: DataFrame  
                the clean dataframe 
        
        '''
        df['email_address'] = df['email_address'].str.replace("@@", "@")
        regex = r"^[\w!#$%&'*+/=?`{|}~^-]+(?:\.[\w!#$%&'*+/=?`{|}~^-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$"
        df['email_address'] = df['email_address'].astype(str)
        df.loc[~df['email_address'].str.match(regex), 'email_address'] = np.nan
        return df
    
    def clean_card_number(self, df):
        df['card_number'] = df['card_number'].apply(self.check_credit_card_length)

    def clean_phone_number(self, df, regex):
        '''
        This function cleans the phone number column.
        '.'
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
            regex: string
                the regular expression for values like +44(0), +49(0) and +1   
        '''
        regex = r'\+44\(0\)|\+1|\+49\(0\)'
        df['phone_number'] = df['phone_number'].apply(lambda x: self.phone_number_plus(regex, x))
        df['phone_number'] = df['phone_number'].replace({r'\(': '', r'\)': ''}, regex=True)
        df['phone_number'] = df['phone_number'].str.replace('.', '').replace(' ','')
        df['phone_number'] = df['phone_number'].str.replace(' ','')


    def clean_non_numerical_data(self, df, column_name):
        '''
        This function cleans the any column where its strictly a string
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
            column_name: string
                the column name
        '''
        df[column_name] = df[column_name].astype(str)
        df.loc[df[column_name].apply(self.check_sum_digit)] = np.nan

    def clean_weight(self, df):
        '''
        This function cleans the weight column 
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
        '''
        df['weight'] = df['weight'].apply(self.convert_to_grams)
        df['weight'] = df['weight'].apply(self.weight_validator)

    def clean_month(self, df):
        '''
        This function cleans the month column 
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
        '''
        df['month'] = df['month'].apply(self.ismonth)

    def clean_year(self, df):
        '''
        This function cleans the year column 
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
        '''
        df['year'] = df['year'].apply(self.isyear)

    def clean_day(self, df):
        '''
        This function cleans the day column 
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
        '''
        df['day'] = df['day'].apply(self.isdate)


    
    # helper functions
    @staticmethod
    def check_sum_digit(string):
        '''
        This check if the number of digits exceeds one
        '''
        return sum(c.isdigit() for c in string) > 1
    
    @staticmethod
    def remove_char_from_digit(df, column_name):
        '''

        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
            column_name: string
                the column name
        '''
        df[column_name] = df[column_name].str.replace('[a-zA-Z]', ' ', regex=True)
        df[column_name] = df[column_name].str.replace('    ', 'NULL') 
        return df
    
    @staticmethod
    def phone_number_plus(regex, phone_number_string):
        '''
        This function cleans values such as +44(0), +49(0) and +1 and 
        subsitute it with 0 or 1

        Parameters:
        ----------  
            regex: string
                the format to match
            phone_number_string: string
                the phone number

        Returns:
        --------
            phone_number_string: string
                cleaned phone_number_string 
        '''
        phone_number_string_match = re.search(regex, phone_number_string)

        if phone_number_string_match and (phone_number_string[:3] == "+44" or phone_number_string[:3] == "+49"):
            return re.sub(regex, "0",phone_number_string)

        elif phone_number_string_match and phone_number_string[:2] == "+1":
            return re.sub(regex, "1",phone_number_string)

        return phone_number_string
    
    def clear_null(self, df, column_name):
        '''
        This function cleans any null values thats in a string format and replace it with
        a NaN value.
        
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
            column_name: string
                the column name
        '''
        df[column_name].replace('NULL', np.nan, inplace=True)
        df[column_name].replace('N/A', np.nan, inplace=True)

    def replace_string(self, df, column_name, string_from, string_to):
        '''
        This function replaces as string with another.
        Parameters:
        ----------  
            df: DataFrame
                the dataframe to be cleaned
            column_name: string
                the column name
            string_from: string
                the string to replace
            string_to: string
                the string to replace the string_from with 
        '''
        df[column_name] = df[column_name].str.replace(string_from, string_to)
        
    @staticmethod
    def check_credit_card_length(card_number):
        '''
        This function checks if the card number length is <= 10, to remove erronous data

        Paramaters:
        -----------
            card_number: int
                the card number in question
        
        Returns:
        --------
            np.nan or card number: [NaN, int]
        '''
        if len(str(card_number)) <= 10:
            return np.nan
        return card_number

    @staticmethod
    def convert_to_grams(metric_string):
        '''
        Converts the weight from their respective metrics to kg

        Paramaters:
        -----------
            metric_string: string
                the weight from the dataframe

        Returns:
        --------
            metric_string: string
                formatted metric in kilograms

        '''
        result = 0
        if 'ml' in metric_string:
            value = metric_string.replace('ml', "")
            result = float(value) / 1000
        elif 'x' in metric_string and 'g' in metric_string:
            x, y = metric_string.split('x')
            x = float(x)
            y = y.replace('.', "")
            y = float(y.replace('g', ""))
            result = (x * y) / 1000
        elif 'g' in metric_string and 'k' not in metric_string:
            value = metric_string.replace('g', "")
            value = value.replace(".", "")
            result = float(value) / 1000
        elif 'oz' in metric_string:
            value = metric_string.replace('oz', "").replace(".", "")
            result = float(value) * 0.0283495

        return metric_string if 'kg' in metric_string else str(result) + "kg" 


    @staticmethod
    def weight_validator(weight_string):
        '''
        Checks if the weights in the weight column follows a similar format. if so we return the
        string else we set it to NaN.

        Parameters:
        -----------
            weight_string: string
                the weight from the weight column
        
        Returns:
        --------
            weight_string or NaN
        '''
        weight_with_x_regex = r'\d+\s*x\s*\d+[g|kg|ml|oz]'
        weight_without_x_regex = r'\d+[g|kg|ml|oz]'

        with_x = re.search(weight_with_x_regex, weight_string)
        without_x = re.search(weight_without_x_regex, weight_string)

        if with_x or without_x:
            return weight_string
        return np.nan
    
    @staticmethod
    def isyear(year_string):
        '''
        This function checks if the year number is an actual year

        Parameters:
        -----------
        year_string: string
            year to be checked
        '''
        if len(str(year_string)) == 4:
            return year_string
        return np.nan
    
    @staticmethod
    def ismonth(month_string):
        '''
        This function checks if the month number is within the range between 1(January)
        and 12 (December).

        Parameters:
        -----------
        month_string: string
            month to be checked
        '''
        try:
            if month_string.isdigit() and int(month_string) <= 12:
                return month_string
        except:
            return np.nan
    
    @staticmethod
    def isdate(day_string):
        '''
        This function checks if the day is an actual date (range between 1-31)

        Parameters:
        -----------
        day_string: string
            day to be checked
        '''
        try:
            if int(day_string) <= 31:
                return day_string
        except ValueError:
            return np.nan