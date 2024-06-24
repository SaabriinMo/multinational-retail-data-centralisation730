from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector



def main():
    data_cleaning = DataCleaning()
    data_extraction = DataExtractor()
    database_connector = DatabaseConnector()

    # sets up the engine
    read_yaml_file = database_connector.read_db_creds('creds/db_creds.yaml')
    engine = database_connector.init_db_engine(read_yaml_file)
    engine.connect()
    
    table_names = database_connector.list_db_tables(engine)

    user_data_df = data_extraction.read_rds_table(table_names[2], engine)

    # clean user data
    user_data_df = data_cleaning.clean_user_data(user_data_df)
    # nulls = user_data_df[user_data_df.isnull().any(axis=1)]

    # clean card details data
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_detail_data_df = data_extraction.retrieve_pdf_data(url)

    card_detail_data_df = data_cleaning.clean_card_data(card_detail_data_df)


    # # get store data and clean it
    number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = data_extraction.list_number_of_stores(number_of_stores_url)
    stores_df = data_extraction.retrieve_stores_data(number_of_stores)
    stores_df = data_cleaning.called_clean_store_data(stores_df)
    # # nulls = stores_df[stores_df.isnull().any(axis=1)]

    # products data and clean it
    product_data_df = data_extraction.extract_from_s3()
    product_data_df = data_cleaning.clean_products_data(product_data_df)
    product_nulls = product_data_df[product_data_df.isnull().any(axis=1)]

    # # clean order table
    order_data_df = data_extraction.read_rds_table(table_names[3], engine)
    order_data_df = data_cleaning.clean_orders_data(order_data_df)
    order_nulls = order_data_df[order_data_df.isnull().any(axis=1)]

    ## clean date event table
    date_event_df = data_extraction.get_date_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    date_event_df = data_cleaning.clean_event_date(date_event_df)

    date_nulls = date_event_df[date_event_df.isnull().any(axis=1)]




    # upload the resulting cleaned dataframe to postgres
    # database_connector.upload_to_db(user_data_df, 'dim_users', 'creds/db_creds.yaml')
    # database_connector.upload_to_db(card_detail_data_df, 'dim_card_details', 'creds/db_creds.yaml')
    # database_connector.upload_to_db(stores_df, 'dim_store_details', 'creds/db_creds.yaml')
    database_connector.upload_to_db(product_data_df, 'dim_products', 'creds/db_creds.yaml')
    database_connector.upload_to_db(order_data_df, 'orders_table', 'creds/db_creds.yaml')
    database_connector.upload_to_db(date_event_df, 'dim_date_times', 'creds/db_creds.yaml')


    return product_nulls, order_nulls, date_nulls


if __name__ == "__main__":
    product_nulls, order_nulls, date_nulls = main()
    # print(product_nulls)
    # print(order_nulls)
    # print(date_nulls)
    # user_data_df, nulls_df = main()
    # print(nulls_df)