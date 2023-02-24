# Função recursiva para acessar vários níveis em dicionário Python

Esse repositório foi criado com a intenção de ajudar os profissionais com lidam com o tratamendo de dados no dia a dia. </br>

<b> Contexto:</b> Ao consultar o endpoint de centro de custos da API do Granatum, um software de gestão financeira empresarial, é retornado um json que 
contém vários níveis, que podem aumentar de acordo com a configuração estabelecida pelo usuário.

<b>Exemplo:</b>
A filial 01 contém o centro de custo Administrativo, que contém o Financeiro e Recursos Humanos. Essa hierarquia não tem um nível definido, o usuário pode
adicionar quantos ele quiser.

<b>Solução:</b>
Precisamos acessar todos os níveis, e para isso criaremos uma função em Python que busca até o último nível, de forma dinâmica, pois não sabemos quantos
níveis existem no JSON, e esse número pode aumentar a qualquer momento.


<h2> Detalhes do Projeto </h2>
Para fins didáticos, utilizaremos um dataframe de exemplo com dados fictícios, que está disponível no repositório.
</br></br>

<b>Informações do Dataframe </b> </br>
Colunas </br>
id: código do centro de custo </br>
descricao: descrição do centro de custo </br>
ativo: True ou False </br>
centros_custo_lucro_filhos: coluna que contém a lista de dicionários </br>

<b> Bibliotecas e funções utilizadas </b> </br>

<b>Pandas:</b>  biblioteca que será utilizada para tratamento de dados </br>
<b>requests:</b> biblioteca que será utilizada para fazer a requisição na API do Granatum </br>
<b>ast:</b> biblioteca que contém a função para extrair o valor de uma string. Ex: Antes "[{}]" e depois [{}] </br>
<b>Função df_recursivo(dataframe):</b> Essa função requer um dataframe como parâmentro, e acessará os níveis da coluna centro de custo e gera um novo Dataframe.
</br>
<b>Função expande_coluna(coluna):</b> Essa funcão será utilizada para expandir a coluna que contém o dicionário.


<b> Contato </b> 

linkedin: https://www.linkedin.com/in/marcelo-menezes-data-enginner/

