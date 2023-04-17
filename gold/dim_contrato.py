import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_dim_contrato():

    # Lendo os dados da camada Silver - Faturamento
    df = pd.read_parquet(
        f'{caminho_silver}/faturamento.parquet',
        columns=['codigo_contrato', 'nome_contrato']
    )

    # Removendo as linhas duplicadas
    df.drop_duplicates(inplace=True)

    # Criando chave surrogada
    df['sk_contrato'] = df.groupby(['codigo_contrato']).ngroup().rank(method='dense').astype(int)

    # removendo index
    df.reset_index(drop=True, inplace=True)

    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/dim_contrato.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df,
        coluna_chave='codigo_contrato'
    )

    # Selecionando colunas na ordem desejada
    df_final = df_final[['sk_contrato', 'codigo_contrato','nome_contrato']]

    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/dim_contrato.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )