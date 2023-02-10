import time

from BWManager import BWManager
from DBManager import DBManager
import Utils

bw = BWManager()
db = DBManager()

bw.login()

year = 2022
month = 9

#download Historical data from month 9 y 2022 to sql
while month >= 0:
    if month == 13:
        month = 1
        year = 2023
    time.sleep(300)
    group_name = 'Banco de Bogotá'
    df = bw.download_group_data_to_df(group_name, month, "false", year)
    df = Utils.get_date_time(df)
    group_name = group_name.replace(" ", "_")
    Utils.charge_sql(db, df, group_name)

    query_name = 'Competidores (Banco de Bogotá) 2023'
    df = bw.download_query_data_to_df(query_name, month, "false", year)
    df = Utils.get_date_time(df)
    query_name = query_name.replace(" ", "_")
    Utils.charge_sql(db, df, query_name)
