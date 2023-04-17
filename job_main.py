# Esse processo executa os arquivos da bronze e silver
import sys
from alerta_gmail import envia_email 

# PROCESSO CAMADA BRONZE
sys.path.append('C:\BI_SOS\ETL_DRE\bronze')
from bronze import (
    bronze_faturamento,
    bronze_categoria,
    bronze_centro_custo,
    bronze_lancamentos
)

def etl_bronze():

    bronze_lancamentos.func_lancamentoBronze()
    bronze_categoria.func_CategoriaBronze()
    bronze_centro_custo.func_CentroCustoBronze()
    bronze_faturamento.func_FaturamentoBronze()

# PROCESSO CAMADA SILVER

sys.path.append('C:\BI_SOS\ETL_DRE\silver')
from silver import (
    silver_acesso_bi,
    silver_categoria,
    silver_centro_custo,
    silver_faturamento,
    silver_lancamentos,
    silver_orcamento
)

def etl_silver():

    silver_acesso_bi.func_BIacessoSilver()
    silver_categoria.func_CategoriaSilver()
    silver_centro_custo.func_centroCustoSilver()
    silver_faturamento.func_FaturamentoSilver()
    silver_lancamentos.func_lancamentoSilver()
    silver_orcamento.func_orcamentoSilver()


# PROCESSO CAMADA GOLD

sys.path.append('C:\BI_SOS\ETL_DRE\gold')
from gold import (
    dim_categoria,
    dim_centro_custo,
    dim_contrato,
    dim_servico,
    dim_unidade,
    fato_faturamento,
    fato_lancamento
)

def etl_gold():

    dim_categoria.func_dim_categoria()
    dim_centro_custo.func_dim_centroCusto()
    dim_contrato.func_dim_contrato()
    dim_servico.func_dim_servico()
    dim_unidade.func_dim_unidade()
    fato_faturamento.func_fato_faturamento()
    fato_lancamento.func_fato_lancamento()


# EXECUTANDO AS FUNÇÕES DE JOB

try:
    etl_bronze()
except Exception as err:
    envia_email(
        assunto='BI DRE - ALERTA ERRO', 
        mensagem=f'Atenção equipe de BI \nHouve um erro no processo da Bronze \n {err}'
    )
else:
    try:
        etl_silver()
    except Exception as err:
        envia_email(
        assunto='BI DRE - ALERTA ERRO', 
        mensagem=f'Atenção equipe de BI \nHouve um erro no processo da Silver \n {err}'
    )
    else:
        try:
            etl_gold()
        except Exception as err:
            envia_email(
                assunto='BI DRE - ALERTA ERRO', 
                mensagem=f'Atenção equipe de BI \nHouve um erro no processo da Gold \n {err}'
            )