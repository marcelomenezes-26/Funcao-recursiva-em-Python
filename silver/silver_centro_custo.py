import sys
sys.path.append('C:\BI_SOS\ETL_DRE')
from bibliotecas import *

def func_centroCustoSilver():

    def df_custo_recursivo(df_base):

        # Colunas utilizadas no Dataframe principal
        colunas_df = ['id', 'descricao', 'parent_id']

        # Verifica se o Dataframe está vazio, caso esteja, encerra a recursividade e retorna os dados do csv
        if df_base.empty == True:
            df = pd.read_csv('C:/BI_SOS/ETL_DRE/temp/centro_custo.csv', sep=',')
            df = df.drop(df.loc[df['id'] == 'id'].index)

            # Remove os dados do arquivo csv para evitar duplicação na próxima carga
            pd.DataFrame(columns=colunas_df).to_csv('C:/BI_SOS/ETL_DRE/temp/centro_custo.csv', mode='w', index=False)
            
            return df
        else:

            # Cria uma lista vazia, que será preenchida na função abaixo
            lista = []

            # Função para acessar a lista de dicionários da coluna "centro de custo"
            def expande_coluna(coluna):

                # Condição para expandir somente as linhas que contém dicionário, por que as demais possuem apenas o valor zero.
                if coluna != []:

                    # Condição para verificar se a lista está dentro de uma string, exemplo: "[{}]".
                    if type(coluna) == str:

                        # Função para extrair apenas a lista dentro da string. Ex: Antes "[{}]" e depois [{}]
                        dados = list(ast.literal_eval(coluna))

                        # Adicionando a lista de dicionários à lista principal, criada anteriormente.
                        lista.extend(dados)

                        # Transformando a lista de dicionários em Dataframe.
                        df = pd.DataFrame(dados, columns=colunas_df)

                        # Adicionando os dados no arquivo csv.
                        df.to_csv('C:/BI_SOS/ETL_DRE/temp/centro_custo.csv', mode='a', index=False, header=False)

                    # Caso a coluna seja do tipo Lista, será aplicado o tratamento abaixo.    
                    else:

                        # Adicionando a lista de dicionários à lista principal, criada anteriormente.
                        lista.extend(coluna)

                        # Transformando a lista de dicionários em Dataframe.
                        df = pd.DataFrame(coluna, columns=colunas_df)

                        # Adicionando os dados no arquivo csv.
                        df.to_csv('C:/BI_SOS/ETL_DRE/temp/centro_custo.csv', mode='a', index=False)


            # Aplicando a função "expande_coluna" no Dataframe
            df_base['centros_custo_lucro_filhos'] = df_base['centros_custo_lucro_filhos'].apply(expande_coluna)

            # Criando o Dataframe que será utilizado na recursividade, ele contém apenas os dados que foram inseridos no loop atual.
            df_atual = pd.DataFrame(lista)

            # Caso o df_novo não esteja vazio, será executada a função novamente para expandir as colunas do próximo nível. 
            return df_custo_recursivo(df_atual)
        
    # Lendo os dados na camada BRONZE
    df_bronze = pd.read_parquet(f'{caminho_bronze}/financeiro/centro_custo.parquet')

    # Criando o Dataframe a partir da função recursiva
    df = df_custo_recursivo(df_bronze)

    # Apagando linhas em branco
    df.dropna(inplace=True)

    # Alterando o tipo da coluna parent_id
    df['id'] = df['id'].astype(int)
    df['parent_id'] = df['parent_id'].astype(int)

    # hierarquia de centro de custo
    df_inicial = df_bronze[['id','descricao']]
    df_base = df_inicial.join(df.set_index('parent_id'), how='inner', on='id', rsuffix='_n2')

    sufixo = 3
    nivel_chave = 2
    count = 1

    while count > 0:    

        # Contagem para verificar se tem o próximo nível 
        df_merge = pd.merge(df, df_base, left_on='parent_id', right_on=f'id_n{nivel_chave}')
        count = df_merge['parent_id'].count()     

        # Realiza o JOIN
        df_base = df_base.join(df.set_index('parent_id'), how='left', on=f'id_n{nivel_chave}', rsuffix=f'_n{sufixo}')

        # Apaga as linhas duplicadas
        df_base.drop_duplicates(keep='first', inplace=True)

        # Sequência do nível
        sufixo += 1
        nivel_chave += 1

        # Remove as colunas em branco
        if df_base[f'id_n{nivel_chave}'].isnull().all() == True:
            df_base.drop(columns=[f'id_n{nivel_chave}', f'descricao_n{nivel_chave}'], inplace=True)


    #Tratando campos nulos
    df_base.fillna('vazio', inplace=True)
    df_base['descricao_n4'].replace('descricao', 'vazio', inplace=True)

    #Criando id único para o nível 2,3 e 4
        
    #Regra nível 2
    df_base['id_temp'] = np.where(
        df_base['id_n3'] == 'vazio', df_base['id_n2'], 'vazio'
    )

    #Regra nível 3 e 4
    df_base['id_temp2'] = np.where(
        df_base['id_n4'] == 'vazio', df_base['id_n3'], df_base['id_n4']
    )

    #Regra geral para a coluna ID
    df_base['id'] = np.where(
        df_base['id_temp'] == 'vazio', df_base['id_temp2'], df_base['id_temp']
    )

    #Selecionando colunas
    df_base = df_base[
        [
            'id',
            'descricao',
            'descricao_n2','descricao_n3','descricao_n4'
        ]
    ]

    # Coluna UNIDADE
    sep_unidade = df_base['descricao'].str.split('.', n=1, expand=True)
    df_base['id_unidade'] = sep_unidade[0].str.strip()

    # Coluna SERVIÇO
    sep_servico = df_base['descricao_n2'].str.split('.', n=1, expand=True)
    df_base['id_servico'] = sep_servico[1].str.extract('^(\d+)')

    # Coluna PROJETO/CLIENTE
    padrao_descricao = r'\((.*?)\)'
    df_base['id_contrato'] = df_base['descricao_n3'].str.extract(padrao_descricao, expand=False)

    padrao_id = r'^(\d+\.\d+\.\d+)'
    df_base['id_contrato'] = df_base['id_contrato'].str.extract(padrao_id, expand=False)

    # Preenchendo coluna vazia
    df_base['id_contrato'].fillna(0, inplace=True)

    # Definindo o tipo das colunas
    df_base['id'] = df_base['id'].astype(int)
    df_base['id_contrato'] = df_base['id_contrato'].astype(str)

    # Enviando os dados para a camada SILVER
    df_base.to_parquet(
        f'{caminho_silver}/centro_custo.parquet',
        compression='snappy',
        engine='fastparquet',
        index=False
    )