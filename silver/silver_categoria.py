import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_CategoriaSilver():

    # A função abaixo ler os dados da planilha DRE_CATEGORIA, que será utilizada no Power BI para o agrupamento da DRE.
    def busca_categoria():

        #Lendo arquivo excel enviado pelo Granatum
        df = pd.read_parquet(f'{caminho_bronze}/financeiro/categorias.parquet')

        #Separando a coluna por delimitador
        sep_categoria = df['CATEGORIAS'].str.split(' - ', n=1, expand=True)

        #Adicionando novas colunas e removendo espaço em branco
        df['id_categoria'] = sep_categoria[0].str.strip() 
        df['desc_categoria'] = sep_categoria[1].str.strip()
        df['agrupamento_dre'] = df['TÓPICO']

        #Removendo colunas antigas
        df.drop(columns=['CATEGORIAS', 'TÓPICO'], inplace=True)

        #Alterando o tipo da coluna id_categoria
        df.id_categoria = pd.to_numeric(df.id_categoria , errors='coerce')
    
        return df

    # Enviando os dados para a camada silver no formato PARQUET
    try:
        df = busca_categoria()
        
        df.to_parquet(
            f'{caminho_silver}/categorias.parquet',
            compression='snappy',
            engine='fastparquet',
            index=False
        )
    except Exception as err:
        print(err)