import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *


# Função para buscar as tags na API Granatum
def busca_tag(endpoint='tags'):

    url = f'{base_url}{endpoint}?{access_token}'
    try:
        response = requests.get(url=url)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(err)
    else:
        df = pd.DataFrame(response.json())

    return df

# Enviando os dados para a camada bronze no formato PARQUET
try:
    df = busca_tag()
    
    df.to_parquet(
        f'{caminho_bronze}/financeiro/tags.parquet',
        compression='snappy',
        engine='fastparquet'
    )
except Exception as err:
    print(err)