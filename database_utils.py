import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd


class DatabaseConnector:
    '''
    Database Connector class provides the bases to connect to the database for data extraction/
    data uploading

    Methods:
    -------
    read_db_creds(yaml_file)
        reads the yaml file and stores the results in a dictonary

    init_db_engine(creds)
        creates the engine to connect to the database

    list_db_tables(engine)
        list the tables in the aws database


    upload_to_db(df, table_name,yaml_file)
        uploads the cleaned dataframe to the database
    
    '''
    def read_db_creds(self, yaml_file):
        '''
        reads the yaml file containing the aws credentials for connecting to the aws database

        Parameters:
        -----------
        yaml_file: str
            location where the yaml file is

        Return:
        -------
        yaml_file: dict
            dictonay containing the credentials to connect the aws database

        Raises:
        -------
            YAMLError: If the file doesnt exists or could not be loaded.
        '''
        try:
            with open(yaml_file, 'r') as file:
                yaml_file = yaml.safe_load(file)
                return yaml_file
        except yaml.YAMLError as e:
                    print(f"Error reading YAML file: {e}")
                    return None
            

    def init_db_engine(self, creds):
        '''
        creates and returns a SQLAlchemy engine for connecting to a PostgreSQL database.


        Parameters:
        -----------
        creds: dict
            dictonary containing the database credentials

        Return:
        -------
        engine: sqlalchemy.engine.Engine
            An SQLAlchemy engine object for the PostgreSQL database.

        Raises:
        -------
        ValueError: If the credentials are not provided or could not be loaded.
        '''
        if creds is None:
            raise ValueError("Credentials are not provided or could not be loaded.")
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self,engine):
        '''
        lists the tables name in the aws database

        Parameters:
        -----------
        engine: sqlalchemy.engine.Engine
            The SQLAlchemy engine connected to the AWS PostgreSQL database.

        Returns:
        --------
        Inspector.get_table_names(): list
            list of table names in the aws database
        '''
        inspector = inspect(engine)
        return inspector.get_table_names()
         
    
    def upload_to_db(self, df, table_name,yaml_file):
        '''
        upload the clean dataframe to postgresql

        Parameters:
        ----------
        df: DataFrame
            the cleaned dataframe

        table_name: str
            the name of the SQL table

        yaml_file: str
            yaml file's location 
        '''
        creds = self.read_db_creds(yaml_file) 
        if creds is None:
            raise ValueError("Credentials are not provided or could not be loaded.")
        engine = create_engine(f"{creds['DATABASE_TYPE_LOCAL']}+{creds['DBAPI_LOCAL']}://{creds['USER_LOCAL']}:{creds['PASSWORD_LOCAL']}@{creds['HOST_LOCAL']}:{creds['PORT_LOCAL']}/{creds['DATABASE_LOCAL']}")
        engine.connect()
        df.to_sql(table_name, engine, if_exists='replace', index=False)


if __name__ == '__main__':
     pass
    # database_connector = DatabaseConnector()
    # read_yaml_file = database_connector.read_db_creds("creds/db_creds.yaml")
    # print(read_yaml_file)
    # engine = database_connector.init_db_engine(read_yaml_file)
    # print(engine.connect())
    # inspector = inspect(engine)
    # new_df = []
    # table_name = inspector.get_table_names()
    # df = database_connector.read_rds_table(table_name[0], engine)
    # print(df)
