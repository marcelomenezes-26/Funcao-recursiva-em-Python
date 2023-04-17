import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_FaturamentoBronze():

    # Chave para acessar o Google Sheets
    key_path = os.path.join(os.getcwd(), chave_gsheets)

    # Seleciona a planilha e intervalo de dados
    spreadsheet_id = '1NYqvjWpqyoCIF7MSAQZGQG4jt3brj-7lXzh8rnvLCCc'
    range_name = 'Banco de Dados!A1:AT'

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

    cols = []
    count = 1
    for column in colunas:
        if column == 'Data Lançamento':
            cols.append(f'Data Lançamento_{count}')
            count+=1
            continue
        cols.append(column)
    
    colunas = cols

    # Criando Dataframe a partir da planilha
    df = pd.DataFrame(
        result['values'],columns=colunas
    ).drop(index=0)
    # Remover o espaço em branco das colunas
    df.rename( 
        columns=
        {
            'Código Contrato': 'codigo_contrato',
            'Nome do Contrato': 'nome_contrato',
            'Código do Centro de Custo': 'codigo_centro_custo',
            'Unidade': 'unidade',
            'Serviço': 'servico',
            'Coordenador': 'coordenador',
            'Gerente do Projeto': 'gerente_projeto',
            'Diretor Responsável': 'diretor_responsavel',
            'Diretoria Responsável': 'diretoria_responsavel',
            'Gerente de Contas': 'gerente_de_contas',
            'Estado': 'estado',
            'Data Competência': 'data_competencia',
            'Ano': 'ano',
            'Mês Competência': 'mes_competencia',
            'Previsão Anual': 'previsao_anual',
            'Previsão atualizada': 'previsao_atualizada',
            'Revisão  Jan/Fev': 'revisao_janeiro_fevereiro',
            'Revisão  Mar/Abril': 'revisao_marco_abril',
            'Revisão  Maio/junho': 'revisao_maio_junho',
            'Revisão  Julho/Agosto': 'revisao_julho_agosto',
            'Revisão  Set/out': 'revisao_setembro_outubro',
            'Revisão Nov/Dez': 'revisao_novembro_dezembro',
            'Revisão Pontual': 'revisao_pontual',
            'Realizado': 'realizado',
            'Não Informado': 'nao_informado',
            'Data Lançamento_1': 'data_lancamento',
            'Envio Termo de Aceite': 'envio_termo_aceite',
            'Termo não enviado': 'termo_nao_enviado',
            'Devolução por Erro': 'devolucao_por_erro',
            'Reenvio Termo Aceite': 'reenvio_termo_aceite',
            'Número Nota Fiscal': 'numero_nota_fiscal',
            'Data Emissão': 'data_emissao',
            'Cliente confirmou': 'cliente_confirmou',
            'Vencimento': 'vencimento',
            'Vencimento Mês': 'vencimento_mes',
            'Previsão Pagamento': 'previsao_pagamento',
            'Data de Pagamento': 'data_pagamento',
            'Valor já recebido': 'valor_recebido',
            'Email enviado?': 'email_enviado',
            'Fatura levada a prejuízo': 'faturada_levada_prejuizo',
            'Reservado para a FOLHA': 'reservado_folha',
            'Valor não Recebido': 'valor_nao_recebido',
            'Valor p/ Emissão NF': 'valor_emissao_nf',
            'Cliente confirmou?': 'cliente_confirmou'
        }, inplace=True
    )

    # Apagando colunas de comentários
    df.drop(
        columns=[
            'Cometários do Setor de Faturamento',
            'Comentários para o Setor de Faturamento',
            'Data Lançamento_2'
        ], 
        inplace=True
    )

    # Enviando para a camada BRONZE no formato Parquet
    try:
        df.to_parquet(
            f'{caminho_bronze}/financeiro/faturamento.parquet',
            engine='fastparquet'
        )
    except Exception as err:
        print(err)