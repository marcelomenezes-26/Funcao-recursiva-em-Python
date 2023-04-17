import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *
from alerta_gmail import envia_email

def func_FaturamentoSilver():

    # Lendo os dados na camada BRONZE
    df = pd.read_parquet(f'{caminho_bronze}/financeiro/faturamento.parquet')

    # Selecionando colunas
    df = df[[
        'codigo_contrato', 'nome_contrato', 'codigo_centro_custo', 
        'unidade','servico', 'estado', 'data_competencia', 'realizado'
    ]]

    # Lendo os dados atuais para verificar divergências
    df_atual = pd.read_parquet(f'{caminho_silver}/faturamento.parquet').shape[0]

    # Verificando se o input na bronze foi correto
    if df.shape[0] >= df_atual:

        # Salvando os dados na camada SILVER
        df.to_parquet(
            f'{caminho_silver}/faturamento.parquet',
            compression='snappy',
            engine='fastparquet',
            index=False
        )
    else:
        envia_email(
        assunto='BI DRE - ALERTA ERRO', 
        mensagem=f'Atenção equipe de BI \nHouve um erro no processo de input da planilha de faturamento'
    )
