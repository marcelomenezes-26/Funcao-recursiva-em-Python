import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_BIacessoSilver():

    # Chave para acessar o Google Sheets
    key_path = os.path.join(os.getcwd(), chave_gsheets)

    # Seleciona a planilha e intervalo de dados
    spreadsheet_id = '1VuILawGhExcyAwsYULG7cdufyGXD3q1fhOdxQJPJZ_c'
    range_name = 'acesso_powerbi!A1:C'

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

    # Enviando os dados atualizados para a camada bronze
    df.to_parquet(
        f'{caminho_silver}/acesso_powerbi.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )