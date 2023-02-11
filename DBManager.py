from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import exc


class DBManager:
    def __init__(self):
        self.server = 'DESKTOP-1K5QK0J\SQLEXPRESS'
        self.database = 'BDB'
        self.conn = None

    def create_or_insert_comp(self, df):
        for retry in range (0,10):
            try:
                connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                                    + 'SERVER=' + self.server \
                                    + ';DATABASE=' + self.database \
                                    + ';Trusted_Connection=yes;'
                connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
                engine = create_engine(connection_url, pool_pre_ping=True)
                df.to_sql("DiarioComp", engine, if_exists='append', index=False)
            except exc.DBAPIError as e:
                print(e)

    def create_or_insert_bdb(self, df):
        for retry in range(0, 10):
            try:
                connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                                    + 'SERVER=' + self.server \
                                    + ';DATABASE=' + self.database \
                                    + ';Trusted_Connection=yes;'
                connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
                engine = create_engine(connection_url, pool_pre_ping=True)
                df.to_sql("DiarioBDB", engine, if_exists='append', index=False)
            except exc.DBAPIError as e:
                print(e)