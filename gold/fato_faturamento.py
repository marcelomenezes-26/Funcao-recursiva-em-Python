import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_fato_faturamento():

    # Lendo os dados da camada Silver - Faturamento
    df = pd.read_parquet(
        f'{caminho_silver}/faturamento.parquet',
        columns=[
        'codigo_contrato', 'codigo_centro_custo',\
        'unidade', 'servico','estado', 'data_competencia',\
        'realizado'    
        ]
    )

    # Removendo linhas em branco
    df.dropna(inplace=True)

    # Coluna id_unidade
    new = df['unidade'].str.split(".", n=1, expand=True)
    df['id_unidade'] = new[0]
    df.drop(columns=['unidade'], inplace=True)

    # Coluna id_servico
    new = df['servico'].str.split(".", n=1, expand=True)
    df['id_servico'] = new[0]
    df.drop(columns=['servico'], inplace=True)

    # Dimensão contrato
    dim_contrato = pd.read_parquet(
        f'{caminho_gold}/dim_contrato.parquet',
        columns=['sk_contrato','codigo_contrato']
    )

    df = df.join(dim_contrato.set_index('codigo_contrato'), how='inner', on='codigo_contrato')

    # Dimensão servico
    dim_servico = pd.read_parquet(
        f'{caminho_gold}/dim_servico.parquet',
        columns=['sk_servico','id_servico']
    )

    df = df.join(dim_servico.set_index('id_servico'), how='inner', on='id_servico')

    # Dimensão unidade
    dim_unidade = pd.read_parquet(
        f'{caminho_gold}/dim_unidade.parquet',
        columns=['sk_unidade','id_unidade']
    )

    df = df.join(dim_unidade.set_index('id_unidade'), how='inner', on='id_unidade')

    # Selecionando colunas
    df = df[
        [
            'data_competencia', 'estado', 'sk_contrato',\
            'sk_unidade', 'sk_servico', 'realizado'
        ]
    ]

    # Salvando os dados na camada GOLD
    df.to_parquet(
        f'{caminho_gold}/fato_faturamento.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )