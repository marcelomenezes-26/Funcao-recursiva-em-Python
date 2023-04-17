from lancamentos import etl_lancamento
import categorias
import centro_custo
from bibliotecas import *
from alerta_gmail import envia_email

#Categoria
try:
    df_categoria = categorias.lista_categoria()
except Exception as err:
    envia_email(
        assunto='BI DRE - ALERTA ERRO', 
        mensagem=f'Atenção equipe de BI \nHouve um erro na tabela categorias \n {err}'
    )
else:
    #Centro de custo
    try:
        df_centro_custo = centro_custo.lista_centro_custo()
    except Exception as err:
        envia_email(
        assunto='BI DRE - ALERTA ERRO', 
        mensagem=f'Atenção equipe de BI \nHouve um erro na tabela centro de custo \n {err}'
    )
    else:
        #Lançamentos
        try:
            #Período para filtrar a data de competência dos lançamentos
            ano_atual = date.today().strftime('%Y')
            dt_inicio = '2023-01-01' #(date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
            dt_final =  f'{ano_atual}-12-31'
            df_lancamento = etl_lancamento(dt_inicio, dt_final)
            
        except Exception as err:
            envia_email(
                assunto='BI DRE - ALERTA ERRO', 
                mensagem=f'Atenção equipe de BI \nHouve um erro na tabela lançamentos \n {err}'
            )

        else:
            
            #Inserindo as dimensões
            with pd.ExcelWriter(f'{caminho}/dimensoes.xlsx', mode='w') as writer:

                df_centro_custo.to_excel(writer, sheet_name='centro_custo', index=False)
                df_categoria.to_excel(writer, sheet_name='categoria', index=False)

            #Deletando o período na base atual
            df = pd.read_excel(f'{caminho}/lancamentos.xlsx', sheet_name='dados')
            selecao = df['data_competencia'] < dt_inicio
            df[selecao].to_excel(f'{caminho}/lancamentos.xlsx', index=False, sheet_name='dados')

            #Inserindo os lançamentos na planilha
            with  pd.ExcelWriter(
                f'{caminho}/lancamentos.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay'
            ) as writer:
            
                start_row = writer.sheets['dados'].max_row
                df_lancamento.to_excel(
                    writer, 
                    sheet_name='dados', 
                    index=False, 
                    startrow=start_row, 
                    header=False
                )

            # Verifica se há categorias no Granatum ainda não configuradas na planilha DRE
            categorias.valida()



            
