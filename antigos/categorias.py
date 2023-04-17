from bibliotecas import *
from alerta_gmail import envia_email

# A função abaixo ler os dados da planilha DRE_CATEGORIA, que será utilizada no Power BI para o agrupamento da DRE.
def lista_categoria():

    #Lendo arquivo excel enviado pelo Granatum
    df = pd.read_excel(f'{caminho}/dre_categoria.xlsx', sheet_name='categorias')

    #Separando a coluna por delimitador
    sep_categoria = df['CATEGORIAS'].str.split(' - ', n=1, expand=True)
    sep_agrupamento = df['TÓPICO'].str.split(')', n=1, expand=True)

    #Adicionando novas colunas e removendo espaço em branco
    df['id_categoria'] = sep_categoria[0].str.strip() 
    df['desc_categoria'] = sep_categoria[1].str.strip()
    df['agrupamento_dre'] = sep_agrupamento[1].str.strip()

    #Removendo colunas antigas
    df.drop(columns=['CATEGORIAS', 'TÓPICO'], inplace=True)

    #Alterando o tipo da coluna id_categoria
    df.id_categoria = pd.to_numeric(df.id_categoria , errors='coerce')
 
    return df


#  A função abaixo verifica as categorias do Granatum que não estão na planilha de agrupamento DRE
def valida():
    
    dt_min = '2023-01-01' #(date.today() - timedelta(days=31)).strftime('%Y-%m-%d')
    df = pd.read_excel(f'{caminho}/lancamentos.xlsx', sheet_name='dados')
    selecao = df['data_competencia'] >= dt_min
    df = df[selecao]
    df = df[['categoria_id']]
    df.drop_duplicates(subset=['categoria_id'], inplace=True)
    df =  df.join(
        lista_categoria().set_index('id_categoria'),
        on='categoria_id', how='left', rsuffix='_b'
    )
    df = df[df['desc_categoria'].isnull()]
    
    def busca_categoria(id_categoria):
        url = f'{base_url}categorias/{id_categoria}?{access_token}'
        response = requests.get(url=url)
        if response.status_code == 200:
            dados = response.json()
            dados = dados['descricao']
        else:
            response.raise_for_status()
        return dados

    dados_lista = []
    for id in df['categoria_id']:
        data = [busca_categoria(id)]
        dados_lista.extend(data)

    df['nome_categoria'] = dados_lista

    # Condição para verificar se há categorias faltando
    if len(df) > 1:
        df.to_excel('temp/categorias_null.xlsx')
        
        # Enviando e-mail de alerta com o DataFrame em anexo
        envia_email(
            assunto='BI DRE - Categorias faltando',
            mensagem=f'Atenção equipe de BI \nEstá faltando categorias na planilha da DRE.',
            anexo='temp/categorias_null.xlsx'
        )
        os.remove('temp/categorias_null.xlsx')

        