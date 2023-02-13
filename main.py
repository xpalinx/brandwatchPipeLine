import time
from BWManager import BWManager
from DBManager import DBManager
import Utils

bw = BWManager()
db = DBManager()

bw.login()
date = Utils.start_date()

while Utils.active_date(date):
    group_name = 'Banco de Bogotá'
    df = bw.download_group_data_to_df(group_name, date, Utils.get_next_day(date))
    df = Utils.get_summary_bdb(df, Utils.get_dia(date), Utils.get_mes(date), Utils.get_año(date))
    print(df.head(5))
    Utils.charge_sql_bdb(db, df)

    query_name = 'Competidores (Banco de Bogotá) 2023'
    df = bw.download_query_data_to_df(query_name, date, Utils.get_next_day(date))
    df = Utils.get_summary_comp(df, Utils.get_dia(date), Utils.get_mes(date), Utils.get_año(date))
    print(df.head(5))
    Utils.charge_sql_comp(db, df)

    date = Utils.get_next_day(date)
    time.sleep(120)
