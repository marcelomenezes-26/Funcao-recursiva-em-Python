import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_CentroCustoBronze():

    # Função para buscar o centro de custo na API Granatum
    def busca_centro_custo():

        # Colunas utilizadas na tabela centro de custo
        colunas_df = ['id', 'descricao', 'ativo', 'centros_custo_lucro_filhos']

        url = f'{base_url}centros_custo_lucro?{access_token}'
        response = requests.get(url=url).json()
        data = pd.DataFrame(response, columns=colunas_df)
        data['centros_custo_lucro_filhos'] = data['centros_custo_lucro_filhos'].astype(str)
        
        return data

    # Enviando os dados para a camada bronze no formato PARQUET
    df = busca_centro_custo()

    try:
        df.to_parquet(
            f'{caminho_bronze}/financeiro/centro_custo.parquet',
            compression='snappy',
            engine='fastparquet',
            index=False
        )
    except Exception as err:
        print(err)
