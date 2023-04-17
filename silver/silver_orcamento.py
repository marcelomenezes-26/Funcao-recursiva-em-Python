import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_orcamentoSilver():

    # Chave para acessar o Google Sheets
    key_path = os.path.join(os.getcwd(), chave_gsheets)

    # Seleciona a planilha e intervalo de dados
    spreadsheet_id = '1TqxcBHy214OSHOKx_EwAHpmD8_oM1Ne1iA7d-TN8U38'
    range_name = 'Planilha1!B1:S'

    # Autenticação utilizando a conta de serviço do Google
    creds = None
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

    # Conectando a API Google Sheets
    service = build('sheets', 'v4', credentials=creds)

    # Buscando os dados da planilha
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()

    # listando colunas da planilha
    colunas = result['values'][0]

    # Criando Dataframe a partir da planilha
    df = pd.DataFrame(result['values'],columns=colunas).drop(index=0)

    # Renomeando colunas com espaço
    df.rename(
        columns= {
            'Centro de Custo Pai': 'centro_custo_pai',
            'Centro de Custo Filho': 'centro_custo_filho',
            'Centro de Custo Filho 1': 'centro_custo_filho_1'
        }, inplace=True
    )

    # Enviando os dados atualizados para a camada bronze
    df.to_parquet(
        f'{caminho_silver}/orcamento_dre.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )