import pandas as pd # biblioteca que será utilizada para tratamento de dados
import requests # biblioteca que será utilizada para fazer a requisição na API do Granatum
import ast # biblioteca que contém a função para extrair o valor de uma string


def df_recursivo(df_base):

    # Colunas utilizadas no Dataframe principal
    colunas_df = ['id', 'descricao', 'parent_id']

    # Verifica se o Dataframe está vazio, caso esteja, encerra a recursividade e retorna os dados do csv
    if df_base.empty == True:
        df = pd.read_csv('dataframe_final.csv', sep=',')

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
                    df.to_csv('dataframe_final.csv', mode='a', index=False)

                # Caso a coluna seja do tipo Lista, será aplicado o tratamento abaixo.    
                else:

                    # Adicionando a lista de dicionários à lista principal, criada anteriormente.
                    lista.extend(coluna)

                    # Transformando a lista de dicionários em Dataframe.
                    df = pd.DataFrame(coluna, columns=colunas_df)

                    # Adicionando os dados no arquivo csv.
                    df.to_csv('dataframe_final.csv', mode='a', index=False)


        # Aplicando a função "expande_coluna" no Dataframe
        df_base['centros_custo_lucro_filhos'] = df_base['centros_custo_lucro_filhos'].apply(expande_coluna)

        # Criando o Dataframe que será utilizado na recursividade, ele contém apenas os dados que foram inseridos no loop atual.
        df_atual = pd.DataFrame(lista)

        # Caso o df_novo não esteja vazio, será executada a função novamente para expandir as colunas do próximo nível. 
        return df_recursivo(df_atual)

        # Final da função

    
# Dataframe inicial, contém o maior nível na hierarquia e será utilizado na função principal
df = pd.read_csv('dataframe_exemplo.csv', sep=';')

# Chamando a função que expande as colunas e cria um novo Dataframe com todos os níveis.
df_final = df_recursivo(df)