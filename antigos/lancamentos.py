from bibliotecas import *

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

def lista_lancamento(dt_inicio,dt_final, endpoint='lancamentos'):

    colunas_df = [
        'id','categoria_id','centro_custo_lucro_id',\
        'descricao','data_competencia','valor','tags'
        ]
    pd.DataFrame(columns=colunas_df).to_csv('temp/lancamentos.csv', sep=',', index=False, mode='w')

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
                df.to_csv('temp/lancamentos.csv', mode='a', index=False, header=False)

                df = pd.read_csv('temp/lancamentos.csv', sep=',')

    os.remove('temp/lancamentos.csv')

    return df

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

def etl_lancamento(dt_inicio, dt_final):

    df = lista_lancamento(dt_inicio=dt_inicio, dt_final=dt_final)

    #Função para extrair o id_tag da coluna no formato dicionário
    def coluna_tag(coluna):
        if len(coluna) > 2:
            coluna = eval(coluna)[0]['id']
        else:
            coluna = 0
        return coluna

    df['id_tag'] = df['tags'].apply(coluna_tag) #Adiciona id_tag a partir da função coluna_tag
    df.drop_duplicates(subset=['id'], inplace=True) #Apaga os duplicados
    df = df[df['categoria_id'] != 495087] #Remover lançamentos de saldo inicial
    df.drop(['tags'], axis=1, inplace=True)

    return df