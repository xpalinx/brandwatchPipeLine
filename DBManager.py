import time

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import exc


class DBManager:
    def __init__(self):
        self.server = ' '
        self.database = 'BDB'
        self.conn = None

    def create_or_insert_comp(self, df):
        inserted = False
        while not inserted:
            try:
                connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                                    + 'SERVER=' + self.server \
                                    + ';DATABASE=' + self.database \
                                    + ';Trusted_Connection=yes;'
                connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
                engine = create_engine(connection_url, pool_pre_ping=True)
                df.to_sql("DiarioComp", engine, if_exists='append', index=False)
                inserted = True
            except exc.DBAPIError as e:
                print(e)
                time.sleep(50)

    def create_or_insert_bdb(self, df):
        inserted = False
        while not inserted:
            try:
                connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                                    + 'SERVER=' + self.server \
                                    + ';DATABASE=' + self.database \
                                    + ';Trusted_Connection=yes;'
                connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
                engine = create_engine(connection_url, pool_pre_ping=True)
                df.to_sql("DiarioBDB", engine, if_exists='append', index=False)
                inserted = True
            except exc.DBAPIError as e:
                print(e)
                time.sleep(50)
