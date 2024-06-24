import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd


class DatabaseConnector:
    def read_db_creds(self, yaml_file):
        try:
            with open(yaml_file, 'r') as file:
                yaml_file = yaml.safe_load(file)
                return yaml_file
        except yaml.YAMLError as e:
                    print(f"Error reading YAML file: {e}")
                    return None
            

    def init_db_engine(self, creds):
        if creds is None:
            raise ValueError("Credentials are not provided or could not be loaded.")
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self,engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
         
    
    def upload_to_db(self, df, table_name,yaml_file):
        creds = self.read_db_creds(yaml_file)
        # engine = self.init_db_engine(creds)
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
