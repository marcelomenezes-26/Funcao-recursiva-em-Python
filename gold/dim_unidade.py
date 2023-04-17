import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *


def func_dim_unidade():

    # Lendo os dados da camada Silver - Faturamento
    df = pd.read_parquet(
        f'{caminho_silver}/faturamento.parquet',
        columns=['unidade']
    )

    # Removendo as linhas duplicadas
    df.drop_duplicates(inplace=True)

    # Removendo linhas em branco
    df.dropna(inplace=True)

    # Separando a coluna por delimitador
    new = df['unidade'].str.split(".", n=1, expand=True)

    # Criando coluna id_unidade e unidade
    df['id_unidade'] = new[0]
    df['nome_unidade'] = new[1]

    # Apagando a coluna antiga "unidade"
    df.drop(columns=['unidade'], inplace=True)

    # Criando chave surrogada
    df['sk_unidade'] = df.groupby(['id_unidade']).ngroup().rank(method='dense').astype(int)

    # removendo index
    df.reset_index(drop=True, inplace=True)

    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/dim_unidade.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df,
        coluna_chave='id_unidade'
    )

    # Selecionando colunas na ordem desejada
    df_final = df_final[['sk_unidade', 'id_unidade','nome_unidade']]


    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/dim_unidade.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )