import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

df = pd.read_excel(f'{caminho_base}/lancamento_historico.xlsx')

# Enviando os dados atualizados para a camada bronze
df.to_parquet(
    f'{caminho_silver}/lancamento_historico.parquet',
    compression='snappy',
    engine='fastparquet',
    index=False
)