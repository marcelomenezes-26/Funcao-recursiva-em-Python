import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_fato_lancamento():

    # Centro de custo SILVER
    df_custo = pd.read_parquet(
        f'{caminho_silver}/centro_custo.parquet', 
        columns=['id','id_unidade','id_servico','id_contrato']
    )

    # Lendo os dados da SILVER
    df_silver = pd.read_parquet(f'{caminho_silver}/lancamentos.parquet')

    # Join para buscar a chave das dimensões
    df = df_silver.join(df_custo.set_index('id'), how='left', on='centro_custo_lucro_id')

    # Dimensão contrato
    dim_contrato = pd.read_parquet(
        f'{caminho_gold}/dim_contrato.parquet',
        columns=['sk_contrato','codigo_contrato']
    )

    df = df.join(dim_contrato.set_index('codigo_contrato'), how='left', on='id_contrato')

    # Dimensão servico
    dim_servico = pd.read_parquet(
        f'{caminho_gold}/dim_servico.parquet',
        columns=['sk_servico','id_servico']
    )

    df = df.join(dim_servico.set_index('id_servico'), how='left', on='id_servico')

    # Dimensão unidade
    dim_unidade = pd.read_parquet(
        f'{caminho_gold}/dim_unidade.parquet',
        columns=['sk_unidade','id_unidade']
    )

    df = df.join(dim_unidade.set_index('id_unidade'), how='left', on='id_unidade')

    # Dimensão centro de custo
    dim_centro_custo = pd.read_parquet(
        f'{caminho_gold}/dim_centro_custo.parquet',
        columns=['id','sk_centro_custo']
    )

    df = df.join(dim_centro_custo.set_index('id'), how='left', on='centro_custo_lucro_id')

    # Dimensão categoria
    dim_categoria = pd.read_parquet(
        f'{caminho_gold}/dim_categoria.parquet',
        columns=['id_categoria', 'sk_categoria']
    )

    df = df.join(dim_categoria.set_index('id_categoria'), how='left', on='categoria_id')

    # Selecionando as colunas 

    df = df[
        [
        'id',
        'id_tag',
        'descricao',
        'data_competencia',
        'sk_contrato',
        'sk_servico',
        'sk_unidade',
        'sk_centro_custo',
        'sk_categoria',
        'valor'
        ]
    ]

    # Preenchendo valores nulos com zero
    df.fillna(0, inplace=True)

    # Tipo das colunas
    df['sk_contrato'] = df['sk_contrato'].astype(int)
    df['sk_servico'] = df['sk_servico'].astype(int)
    df['sk_unidade'] = df['sk_unidade'].astype(int)
    df['sk_centro_custo'] = df['sk_centro_custo'].astype(int)
    df['sk_categoria'] = df['sk_categoria'].astype(int)


    # Lendo os dados atuais
    df_gold = pd.read_parquet(f'{caminho_gold}/fato_lancamento.parquet')

    # Realizando o upsert
    df_final = upsert_df(
        df_atual=df_gold,
        df_novo=df,
        coluna_chave='id'
    )

    # Salvando os dados na camada GOLD
    df_final.to_parquet(
        f'{caminho_gold}/fato_lancamento.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )
