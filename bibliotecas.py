from datetime import date, timedelta
from operator import imod
import os
import shutil
import time
import requests
import json
import pandas as pd
import ast
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build
import fastparquet
import psycopg2

#Pasta arquivos BI_DRE
caminho_base = 'G:/Meu Drive/BI_DRE/painel_financeiro/base'

caminho_bronze = 'G:/Meu Drive/DATA_LAKE_SOS/bronze'
caminho_silver = 'G:/Meu Drive/DATA_LAKE_SOS/silver/financeiro'
caminho_gold = 'G:/Meu Drive/DATA_LAKE_SOS/gold/financeiro'

#Credenciais utilizadas
credenciais = pd.read_csv(f'{caminho_bronze}/credenciais/acessos_bi.txt', sep=';')
chave_gsheets = f'{caminho_bronze}/credenciais/bi-sos-382417-1edfef5b79b0.json'

#API Granatum
token = str(credenciais['token'][0])
base_url = 'https://api.granatum.com.br/v1/'
access_token = f'access_token={token}'

# Email de alerta
user = credenciais['email'][0]
password = credenciais['senha'][0]


# Função para fazer UPSERT
def upsert_df(df_atual, df_novo, coluna_chave):

    colunas_df = list(df_atual.columns)

    merged_df = pd.merge(df_atual, df_novo, on=coluna_chave, how='outer', suffixes=('_left', '_right'), indicator=True)

    # Função para determinar o valor atualizado de cada coluna
    def update_value(left_val, right_val):
        if pd.notnull(right_val):
            return right_val
        else:
            return left_val

    # Aplicando a função de update nas colunas, exceto a coluna id_lancamento
    cols_to_update = [col for col in colunas_df if col != coluna_chave]
    for col in cols_to_update:
        merged_df[col] = merged_df.apply(lambda row: update_value(row[col + '_left'], row[col + '_right']), axis=1)

    # Apagando as colunas do dataframe com dados atuais
    merged_df.drop(
        [col + '_left' for col in cols_to_update] + [col + '_right' for col in cols_to_update], axis=1, inplace=True
    )

    # Filtrando linhas que não serão atualizadas
    upserted_df = merged_df.loc[merged_df['_merge'] != 'left_only']

    # Apagando a coluna "_merge"
    upserted_df.drop('_merge', axis=1, inplace=True)

    return upserted_df