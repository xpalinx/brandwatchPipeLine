import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def start_date():
    today = datetime.now()
    five_months_ago = today + relativedelta(months=-5)
    return datetime(five_months_ago.year, five_months_ago.month, 1)


def today():
    return datetime.now()

def get_next_day(current_date):
    current_date += timedelta(days=1)
    return current_date


def get_dia(current_date):
    return current_date.day


def get_mes(current_date):
    return current_date.month


def get_año(current_date):
    return current_date.year


def active_date(current_date):
    today = datetime.now()
    if current_date <= today:
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
    chunks = split_dataframe(df)
    for c in chunks:
        db.create_or_insert_comp(c)


def charge_sql_bdb(db, df):
    chunks = split_dataframe(df)
    for c in chunks:
        db.create_or_insert_bdb(c)


def get_summary_comp(df, dia, mes, año):
    columns_to_insert = ['dia', 'mes', 'año', 'marca', '%Positivo', '%Neutro', '%Negativo', 'total', 'impresiones',
                         'impacto']
    bancos = ['Bancolombia', 'BBVA', 'Davivienda', 'Scotiabank Colpatria', 'Banco de Bogotá', 'Daviplata', 'Nequi']

    df.columns = df.columns.str.lower()
    df['categorydetails'] = df['categorydetails'].astype(str)
    df['dia'] = dia
    df['mes'] = mes
    df['año'] = año
    # Initialize an empty DataFrame with the desired columns
    df_new = pd.DataFrame(columns=columns_to_insert)
    try:
        for banco in bancos:
            # Filter the rows in the original DataFrame where the 'category' column contains the current bank
            df_filtered = df[df['categorydetails'].str.contains(banco, case=False, na=False)]
            pos = df_filtered['sentiment'].eq('positive').mean()
            neg = df_filtered['sentiment'].eq('negative').mean()
            neu = df_filtered['sentiment'].eq('neutral').mean()
            impresiones = df_filtered['impressions'].sum()
            impacto = df_filtered['impact'].sum()
            total = df_filtered.shape[0]
            row = [dia, mes, año, banco, pos, neg, neu, total, impresiones, impacto]
            df_new = pd.concat([df_new, pd.DataFrame([row], columns=columns_to_insert)], ignore_index=True)
            print("appended rows ", row)
        return df_new
    except Exception as e:
        print("exception ", e)


def get_summary_bdb(df, dia, mes, año):
    columns_to_insert = ['dia', 'mes', 'año', '%Positivo', '%Neutro', '%Negativo', 'total', 'impresiones',
                         'impacto']

    df.columns = df.columns.str.lower()
    df['dia'] = dia
    df['mes'] = mes
    df['año'] = año

    # Initialize an empty DataFrame with the desired columns
    df_new = pd.DataFrame(columns=columns_to_insert)
    try:
        pos = df['sentiment'].eq('positive').mean()
        neg = df['sentiment'].eq('negative').mean()
        neu = df['sentiment'].eq('neutral').mean()
        impresiones = df['impressions'].sum()
        impacto = df['impact'].sum()
        total = df.shape[0]
        row = [dia, mes, año, pos, neg, neu, total, impresiones, impacto]
        df_new = pd.concat([df_new, pd.DataFrame([row], columns=columns_to_insert)], ignore_index=True)
        print("appended rows ", row)
        return df_new
    except Exception as e:
        print("exception ", e)


def calcular_tiempos_de_respuesta(df):
    df['added'] = pd.to_datetime(df['added'])
    url_to_datetime = dict(zip(df['url'], df['added']))
    df['response_time'] = df['added'] - df['replyto'].map(url_to_datetime)
    df = df[df['replyto'].notnull()]
    response_times = df['response_time'].agg(['mean', 'min', 'max'])
    return response_times


def check_nat(df):
    df.dropna(inplace=True)
    df.fillna(value='1900-01-01', inplace=True)
    print(df.head(5))
