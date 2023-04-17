import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_dim_centroCusto():

    # Lendo os dados da SILVER
    df_silver = pd.read_parquet(
        f'{caminho_silver}/centro_custo.parquet',
        columns=['id', 'descricao', 'descricao_n2', 'descricao_n3','descricao_n4']
    )

    # Removendo as linhas duplicadas
    df_silver.drop_duplicates(inplace=True)

    # Criando chave surrogada
    df_silver['sk_centro_custo'] = df_silver.groupby(['id']).ngroup().rank(method='dense').astype(int)

    # removendo index
    df_silver.reset_index(drop=True, inplace=True)

    # Ordenando colunas
    df_silver = df_silver[
        [
        'sk_centro_custo',
        'id',
        'descricao',
        'descricao_n2',
        'descricao_n3',
        'descricao_n4'   
        ]
    ]

    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/dim_centro_custo.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df_silver,
        coluna_chave='id'
    )

    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/dim_centro_custo.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )