import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_lancamentoBronze():

    # Colunas utilizadas na tabela de lançamentos
    colunas_df = [
            'id','categoria_id','centro_custo_lucro_id',\
            'descricao','data_competencia','data_vencimento',\
            'data_pagamento','valor','tags'
            ]

    # Função para buscar as contas bancárias
    def conta_bancaria(endpoint='contas'):
        
        url = f'{base_url}{endpoint}?{access_token}'
        try:
            response = requests.get(url=url)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            print(err)
        else:
            df = pd.DataFrame(response.json())
            df = df.query('permite_lancamento == True')
            df = df['id']

        return df

    # Função para buscar os lançamentos na API do Granatum
    def busca_lancamento(dt_inicio,dt_final, endpoint='lancamentos'):

        pd.DataFrame(columns=colunas_df).to_csv('C:/BI_SOS/ETL_DRE/temp/lancamentos.csv', sep=',', index=False, mode='w')

        for conta_id in conta_bancaria():
            url = f'{base_url}{endpoint}?{access_token}&conta_id={conta_id}'
            url += f'&data_inicio={dt_inicio}&data_fim={dt_final}'

            #count: retorna a qtde de lançamentos
            url += '&regime=competencia&tipo=DT&tipo_view=count'  
            try:
                response = requests.get(url=url)
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                print(err)
            else:
                qtde_lancamentos = requests.get(url=url).json()['0']
                
                pagina = -200
                while (qtde_lancamentos-pagina) >= 200:
                    pagina += 200
                    url = f'{base_url}{endpoint}?{access_token}&conta_id={conta_id}'
                    url += f'&data_inicio={dt_inicio}&data_fim={dt_final}'
                    url += '&regime=competencia&tipo=DT'
                    url += f'&limit=200&start={pagina}'
                    response = requests.get(url=url)
                    df = pd.DataFrame(response.json(),columns=colunas_df)
                    df.to_csv('C:/BI_SOS/ETL_DRE/temp/lancamentos.csv', mode='a', index=False, header=False)
                    
                    df = pd.read_csv('C:/BI_SOS/ETL_DRE/temp/lancamentos.csv', sep=',')

        os.remove('C:/BI_SOS/ETL_DRE/temp/lancamentos.csv')

        return df

    # Lendo os dados novos
    ano_atual = date.today().strftime('%Y')
    dt_inicio = '2023-01-01' #(date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    dt_final =  f'{ano_atual}-12-31'
    df_new = busca_lancamento(dt_inicio, dt_final)

    
    # Enviando os dados atualizados para a camada bronze
    df_new.to_parquet(
        f'{caminho_bronze}/financeiro/lancamentos.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )