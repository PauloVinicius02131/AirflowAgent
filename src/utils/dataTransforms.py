# Objetivo do arquivo : Funções de transformação de dados genericas.
import pandas as pd

def format_date_english_cigam(date_columns, df):
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    return df

def format_numeric_cigam(numeric_columns, df):
    for column in numeric_columns:
        df[column] = df[column].astype(str).str.replace(',', '', regex=True)
    return df

def format_numeric_english(numeric_columns, df):
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce').round(2)
    return df

def converter_datetime_formato_english(datetime_columns, df):
    for column in datetime_columns:
        df[column] = pd.to_datetime(df[column], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def format_date_english(date_columns, df):
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    return df

def format_datetime_english(datetime_columns, df):
    for column in datetime_columns:
        df[column] = pd.to_datetime(df[column],  format='%d/%m/%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def format_datetime_debitointerno(datetime_columns, df):
    for column in datetime_columns:
        df[column] = pd.to_datetime(df[column],  format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def format_date_debitointerno(date_columns, df):
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
    return df

def decimal_to_time(decimal_time):
    if decimal_time is not None:
        hours = int(decimal_time)
        minutes = int((decimal_time - hours) * 60)
        return f"{hours:02d}:{minutes:02d}"
    else:
        return None

# converter duração minutos em hh:mm ex 32 = 00:32
def minutes_to_hhmm(duracao):
    if duracao is not None:
        horas = int(duracao / 60)
        minutos = int(duracao % 60)
        return f"{horas:02d}:{minutos:02d}"
    else:
        return None

def converter_vazio_para_null(valor):
    if valor == '' or valor == ' ' or valor == '  ':
        return None
    else:
        return valor

class cg:
    def __init__(self) -> None:
        pass

    def converter_duracao_para_minutos(self,duracao):
        # se duração é vazio ou nulo, retorna 0
        if not duracao or pd.isnull(duracao):
            return 0
        
        duracao_split = duracao.split(" ")
        dias = int(duracao_split[0].split("d")[0])
        horas = int(duracao_split[1].split("h")[0])
        minutos = int(duracao_split[2].split("m")[0])

        if len(duracao_split) > 3:
            segundos = int(duracao_split[3].split("s")[0])
        else:
            segundos = 0

        duracao_minutos = dias * 24 * 60 + horas * 60 + minutos + segundos / 60
        return duracao_minutos
