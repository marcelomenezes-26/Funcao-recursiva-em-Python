import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_dim_categoria():

    df = pd.read_parquet(f'{caminho_silver}/categorias.parquet')

    # Removendo as linhas duplicadas
    df.drop_duplicates(inplace=True)

    # Criando chave surrogada
    df['sk_categoria'] = df.groupby(['id_categoria']).ngroup().rank(method='dense').astype(int)

    # removendo index
    df.reset_index(drop=True, inplace=True)

    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/dim_categoria.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df,
        coluna_chave='id_categoria'
    )

    #ordenando colunas 
    df_final = df_final[
        [
            'sk_categoria',
            'id_categoria',
            'desc_categoria',
            'agrupamento_dre'
        ]
    ]


    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/dim_categoria.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )