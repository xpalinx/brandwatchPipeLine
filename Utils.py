import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def start_date():
    today = datetime.now()
    five_months_ago = today + relativedelta(months=-5)
    return datetime(five_months_ago.year, five_months_ago.month, 1)


def get_next_day(current_date):
    current_date += timedelta(days=1)
    return current_date


def get_dia(current_date):
    return current_date.day


def get_mes(current_date):
    return current_date.month


def is_today(current_date):
    today = datetime.now()
    if current_date == today:
        return True
    else:
        return False


def json_to_df(jsonobj):
    df = pd.DataFrame(jsonobj)
    return df


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


def charge_sql_comp(db, df):
    df["category"] = df["category"].astype(str)
    chunks = split_dataframe(df)
    for c in chunks:
        db.create_or_insert_comp(c)


def charge_sql_bdb(db, df):
    chunks = split_dataframe(df)
    for c in chunks:
        db.create_or_insert_bdb(c)


def get_columns_comp(df, dia, mes):
    df.columns = df.columns.str.lower()
    df['dia'] = dia
    df['mes'] = mes
    df = df.assign(alcance=df['impact'] * df['impressions'] / 100)
    columns_rename = {'sentiment': 'sentimiento',
                      'author': 'autor',
                      'twitterfollowers': 'seguidores',
                      'title': 'titulo',
                      'categorydetails': 'category'
                      }
    df.rename(columns=columns_rename, inplace=True)
    columns_to_insert = ['sentimiento', 'dia', 'mes', 'url', 'autor', 'seguidores', 'titulo', 'alcance', 'category']
    df_to_insert = df.loc[:, columns_to_insert]
    return df_to_insert


def get_columns_bdb(df, dia, mes):
    df.columns = df.columns.str.lower()
    df['dia'] = dia
    df['mes'] = mes
    df = df.assign(alcance=df['impact'] * df['impressions'] / 100)
    columns_rename = {'sentiment': 'sentimiento',
                      'author': 'autor',
                      'avatarurl': 'avatar',
                      'twitterfollowers': 'seguidores',
                      'title': 'titulo',
                      'categorydetails': 'category'
                      }
    df.rename(columns=columns_rename, inplace=True)
    columns_to_insert = ['sentimiento', 'dia', 'mes', 'avatar', 'url', 'autor', 'seguidores', 'titulo', 'alcance']
    df_to_insert = df.loc[:, columns_to_insert]
    return df_to_insert
