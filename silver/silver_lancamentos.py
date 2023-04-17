import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_lancamentoSilver():

    # Lendo os lançamentos na BRONZE
    df_bronze = pd.read_parquet(f'{caminho_bronze}/financeiro/lancamentos.parquet')

    #Função para extrair o id_tag da coluna no formato dicionário
    def coluna_tag(coluna):
        if len(coluna) > 2:
            coluna = eval(coluna)[0]['id']
        else:
            coluna = 0
        return coluna

    #Adiciona id_tag a partir da função coluna_tag
    df_bronze['id_tag'] = df_bronze['tags'].apply(coluna_tag)

    #Apaga os duplicados
    df_bronze.drop_duplicates(subset=['id'], inplace=True)

    #Remover lançamentos de saldo inicial
    df_bronze = df_bronze[df_bronze['categoria_id'] != 495087]

    # Removendo a coluna tags
    df_bronze.drop(['tags'], axis=1, inplace=True)

    # Lendo os dados atuais
    df_silver = pd.read_parquet(f'{caminho_silver}/lancamentos.parquet')

    # Deletando linhas que foram apagadas no Granatum
    df_silver = df_silver[df_silver['id'].isin(df_bronze['id'])]

    # Realizando o upsert
    df_lancamento = upsert_df(
        df_atual = df_silver,
        df_novo=  df_bronze,
        coluna_chave = 'id'
    )

    # Salvando os dados na camada SILVER
    df_lancamento.to_parquet(
        f'{caminho_silver}/lancamentos.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )