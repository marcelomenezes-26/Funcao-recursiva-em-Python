import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_dim_servico():

    # Lendo os dados da camada Silver - Faturamento
    df = pd.read_parquet(
        f'{caminho_silver}/faturamento.parquet',
        columns=['servico']
    )

    # Removendo as linhas duplicadas
    df.drop_duplicates(inplace=True)

    # Removendo linhas em branco
    df.dropna(inplace=True)

    # Separando a coluna por delimitador
    new = df['servico'].str.split(".", n=1, expand=True)

    # Criando coluna id_servico e servico
    df['id_servico'] = new[0]
    df['nome_servico'] = new[1]

    # Apagando a coluna antiga "servico"
    df.drop(columns=['servico'], inplace=True)

    # Criando chave surrogada
    df['sk_servico'] = df.groupby(['id_servico']).ngroup().rank(method='dense').astype(int)

    # removendo index
    df.reset_index(drop=True, inplace=True)

    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/dim_servico.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df,
        coluna_chave='id_servico'
    )

    # Selecionando colunas na ordem desejada
    df_final = df_final[['sk_servico', 'id_servico','nome_servico']]


    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/dim_servico.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )