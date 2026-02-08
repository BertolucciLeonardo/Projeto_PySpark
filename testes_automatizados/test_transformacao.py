import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from main import get_resumo_clientes, get_balanco_produtos, join_data

# ==============================================================================
# CRIACAO DE SESSAO SPARK
# ==============================================================================

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Testes_Pipeline_ETL") \
        .getOrCreate()

# ==============================================================================
# TESTE DE TODAS AS ETAPAS DO SCRIPT USANDO REGISTROS DOS ARQUIVOS DE ORIGEM.
# ==============================================================================

def test_pipeline_completo_com_dados_mock(spark):
    
    # 1. Validando o fatiamento do arquivo (.TXT).
    linha_txt = "0000100001000010001000020260207"
    df_raw = spark.createDataFrame([(linha_txt,)], ["value"])

    df_vendas = df_raw.select(
        F.substring("value", 1, 5).alias("venda_id"),
        F.substring("value", 6, 5).cast("int").alias("cliente_id"),
        F.substring("value", 11, 5).alias("produto_id"),
        (F.substring("value", 16, 8).cast("double") / 100).alias("valor"),
        F.substring("value", 24, 8).alias("data_venda")
    )

    # VERIFICACAO DE ESTRUTURA DO ARQUIVO (.TXT).
    amostra = df_vendas.first()
    assert amostra["venda_id"] == "00001", "Erro no fatiamento da venda_id"
    assert amostra["cliente_id"] == 1, "Erro no cast de cliente_id para int"
    assert amostra["produto_id"] == "00001", "Erro no fatiamento do produto_id"
    assert amostra["valor"] == 100.00, "Erro na conversão do valor decimal"
    assert amostra["data_venda"] == "20260207", "Erro no fatiamento da data"

    # 2. VALIDANDO O JOIN ENTRE CLIENTES E VENDAS.
    df_clientes = spark.createDataFrame([(1, "Joao Silva")], ["cliente_id", "nome"])
    df_unificado = join_data(df_clientes, df_vendas)
    
    assert df_unificado.count() == 1
    assert "nome" in df_unificado.columns
    assert df_unificado.first()["nome"] == "Joao Silva"

    # 3. VALIDACAO DO RESUMO DE CLIENTES (Soma, Média e Arredondamento).
    # FOI ADICIONADO MAIS UMA VENDA PARA O MESMO CLIENTE PARA FAZER O TESTE DA AGREGACAO.
    venda_2 = [("00002", 1, "00002", 50.55, "20260207")]
    df_vendas_extra = spark.createDataFrame(venda_2, ["venda_id", "cliente_id", "produto_id", "valor", "data_venda"])
    df_final_vendas = df_vendas.union(df_vendas_extra)
    
    # REFAZENDO O JOIN COM AS DUAS VENDAS.
    df_unificado_full = join_data(df_clientes, df_final_vendas)
    
    resumo_cli = get_resumo_clientes(df_unificado_full)
    res_cli = resumo_cli.first()

    # VALIDACAO DOS CALCULOS (100.00 + 50.55 = 150.55).
    assert res_cli["total_vendas"] == 150.55
    assert res_cli["quantidade_vendas"] == 2
    assert res_cli["ticket_medio"] == 75.28 # (150.55 / 2) arredondado

    # 4. VALIDACAO DO BALANDO DE PRODUTOS.
    balanco_prod = get_balanco_produtos(df_final_vendas)
    
    # VALIDACAO DO PRODUTO (produto_id = 00001 que teve apenas 1 venda de 100.00).
    res_prod = balanco_prod.filter(F.col("produto_id") == "00001").first()
    assert res_prod["total_vendas_produto"] == 100.00
    assert res_prod["quantidade_vendas_produto"] == 1