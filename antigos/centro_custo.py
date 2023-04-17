from bibliotecas import *

colunas_df_nivel = ['id', 'descricao', 'parent_id', 'centros_custo_lucro_filhos']

def consulta_api():

    url = f'{base_url}centros_custo_lucro?{access_token}'
    response = requests.get(url=url).json()
    df = pd.DataFrame(response)
    return df

def nivel2():
    pd.DataFrame(columns=colunas_df_nivel).to_csv('temp/nivel2.csv', sep=',', index=False)
    df = pd.DataFrame(consulta_api())
    i = -1
    for id in df['id']:
        i += 1
        df_nivel2 = pd.DataFrame(df['centros_custo_lucro_filhos'][i], columns=colunas_df_nivel)
        df_nivel2.to_csv('temp/nivel2.csv', mode='a', index=False, header=False)
    df_nivel2 = pd.DataFrame(pd.read_csv('temp/nivel2.csv', sep=','))
    os.remove('temp/nivel2.csv')

    return df_nivel2

def nivel3():
    pd.DataFrame(columns=colunas_df_nivel).to_csv('temp/nivel3.csv', sep=',', index=False)
    df = pd.DataFrame(nivel2())
    i = -1
    for id in df['id']:
        i += 1
        if len(df['centros_custo_lucro_filhos'][i]) > 2:
            dados = df['centros_custo_lucro_filhos'][i]
            dados = list(ast.literal_eval(dados))
            df_nivel3 = pd.DataFrame(dados, columns=colunas_df_nivel)
            df_nivel3.to_csv('temp/nivel3.csv', mode='a', index=False, header=False)
    df_nivel3 = pd.DataFrame(pd.read_csv('temp/nivel3.csv', sep=','))
    os.remove('temp/nivel3.csv')

    return df_nivel3

def nivel4():
    pd.DataFrame(columns=colunas_df_nivel).to_csv('temp/nivel4.csv', sep=',', index=False)
    df = pd.DataFrame(nivel3())
    i = -1
    for id in df['id']:
        i += 1
        if len(df['centros_custo_lucro_filhos'][i]) > 2:
            dados = df['centros_custo_lucro_filhos'][i]
            dados = list(ast.literal_eval(dados))
            df_nivel4 = pd.DataFrame(dados, columns=colunas_df_nivel)
            df_nivel4.to_csv('temp/nivel4.csv', mode='a', index=False, header=False)
    df_nivel4 = pd.DataFrame(pd.read_csv('temp/nivel4.csv', sep=','))
    os.remove('temp/nivel4.csv')

    return df_nivel4

def lista_centro_custo():

    colunas_df = ['id', 'descricao', 'parent_id']
    
    #Criando DataFrame para cada nível
    df_1 = pd.DataFrame(consulta_api(), columns=['id', 'descricao'])
    df_2 = pd.DataFrame(nivel2(), columns=colunas_df)
    df_3 = pd.DataFrame(nivel3(), columns=colunas_df)
    df_4 = pd.DataFrame(nivel4(), columns=colunas_df)

    #Join com os demais níveis do centro de custo
    df = df_1.join(df_2.set_index('parent_id'), how='inner', on='id', rsuffix='_n2')
    df = df.join(df_3.set_index('parent_id'), how='left', on='id_n2', rsuffix='_n3')
    df = df.join(df_4.set_index('parent_id'), how='left', on='id_n3', rsuffix='_n4')

    #Tratando campos nulos
    df.fillna('vazio', inplace=True)

    #Criando id único para o nível 2,3 e 4
    
    #Regra nível 2
    df['id_temp'] = np.where(
        df['id_n3'] == 'vazio', df['id_n2'], 'vazio'
    )

    #Regra nível 3 e 4
    df['id_temp2'] = np.where(
        df['id_n4'] == 'vazio', df['id_n3'], df['id_n4']
    )

    #Regra geral para a coluna ID
    df['id'] = np.where(
        df['id_temp'] == 'vazio', df['id_temp2'], df['id_temp']
    )

    #Selecionando colunas
    df = df[
        [
            'id',
            'descricao',
            'descricao_n2','descricao_n3','descricao_n4'
        ]
    ]

    return df