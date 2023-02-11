import time

from BWManager import BWManager
from DBManager import DBManager
import Utils

bw = BWManager()
db = DBManager()

bw.login()
date = Utils.start_date()
while not Utils.is_today(date):
    tries = 10
    for i in range(tries):
        try:
            group_name = 'Banco de Bogotá'
            df = bw.download_group_data_to_df(group_name, date, Utils.get_next_day(date))
            df = Utils.get_columns_bdb(df, Utils.get_dia(date), Utils.get_mes(date))
            Utils.charge_sql_bdb(db, df)
        except KeyError as e:
            if i < tries - 1:
                continue
            else:
                raise
        break

    for i in range(tries):
        try:
            query_name = 'Competidores (Banco de Bogotá) 2023'
            df = bw.download_query_data_to_df(query_name, date, Utils.get_next_day(date))
            df = Utils.get_columns_comp(df, Utils.get_dia(date), Utils.get_mes(date))
            Utils.charge_sql_comp(db, df)
        except KeyError as e:
            if i < tries - 1:  # i is zero indexed
                continue
            else:
                raise
        break

    date = Utils.get_next_day(date)
    time.sleep(200)
