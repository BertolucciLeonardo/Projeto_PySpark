from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum, count, avg, round
import os

# ==============================================================================
# PARAMETRIZACAO DE VARIAVEIS E CRIACAO DE SESSAO SPARK
# ==============================================================================

# 1. DESCOBRE O CAMINHO DA PASTA ONDE ESTE SCRIPT (.PY) ESTÁ SALVO.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. MONTA OS CAMINHOS BASEADO NA ESTRUTURA DE PASTAS DO PROJETO.
PATH_CLIENTES = os.path.join(BASE_DIR, "data", "clientes.csv")
PATH_VENDAS   = os.path.join(BASE_DIR, "data", "vendas.txt")
PATH_OUTPUT   = os.path.join(BASE_DIR, "output")

# 3. PARAMETRIZA O DELIMITADOR DO ARQUIVO (.CSV).
DELIMITADOR_CSV = "," 

# 4. CRIACAO DA SESSAO SPARK.
def create_spark_session():
    return SparkSession.builder \
        .appName("Projeto_ETL_Spark") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

# ==============================================================================
# LEITURA DOS ARQUIVOS
# ==============================================================================

# 1. LEITURA DO ARQUIVO (.CSV) DE CLIENTE.

def extract_CSV(spark, clientes_path):
    print(f"Lendo arquivo de: {clientes_path}")
    return spark.read.csv(clientes_path, header=True, inferSchema=True, sep=DELIMITADOR_CSV)

# 2. LEITURA POSICIONAL DO ARQUIVO (.TXT) DE VENDAS.
def extract_TXT(spark, vendas_path):
    print(f"Lendo arquivos de: {vendas_path}")
    raw_vendas = spark.read.text(vendas_path)
    return raw_vendas.select(
        substring(col("value"), 1, 5).alias("venda_id"),
        substring(col("value"), 6, 5).cast("int").alias("cliente_id"),
        substring(col("value"), 11, 5).alias("produto_id"),
        (substring(col("value"), 16, 8).cast("double") / 100).alias("valor"),
        substring(col("value"), 24, 8).alias("data_venda")
    )

# ==============================================================================
# TRANSFORMACAO DOS DADOS
# ==============================================================================

# 1. JOIN ENTRE AS BASES DE CLIENTE E VENDAS.
def join_data(df_clientes, df_vendas):
    print("Realizando Join de Clientes e Vendas...")
    return df_vendas.join(df_clientes, on="cliente_id", how="inner")

# 2. GERAÇÃO DO RESUMO DE CLIENTES.
def get_resumo_clientes(df_joined):
    print("Gerando resumo por cliente...")
    return df_joined.groupBy("cliente_id", "nome", "data_venda").agg(
        round(sum("valor"), 2).alias("total_vendas"),
        count("venda_id").alias("quantidade_vendas"),
        round(avg("valor"), 2).alias("ticket_medio")
    )

# 3. GERAÇÃO DO BALANÇO DE PRODUTOS.
def get_balanco_produtos(df_vendas):
    print("Gerando balanço por produto...")
    return df_vendas.groupBy("produto_id", "data_venda").agg(
        round(sum("valor"), 2).alias("total_vendas_produto"),
        count("venda_id").alias("quantidade_vendas_produto"),
        round(avg("valor"), 2).alias("ticket_medio_produto")
    )

# ==============================================================================
# CARREGAMENTO DOS DADOS
# ==============================================================================

# 1. CRIACAO DOS DIRETORIOS, PARTICOES E AQUIVOS.
def load_data(df, path, prefixo_arquivo, partition_col="data_venda"):
    try:
        df.repartition(1).write.mode("overwrite") \
            .option("header", "true") \
            .partitionBy(partition_col) \
            .csv(path)
        
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".csv") and file.startswith("part-"):
                    pasta_data = os.path.basename(root)
                    valor_data = pasta_data.split("=")[-1]
                    novo_nome = f"{prefixo_arquivo}_{valor_data}.csv"
                    os.rename(os.path.join(root, file), os.path.join(root, novo_nome))
      
        print(f"Sucesso: Dados salvos em {path}")
    except Exception as e:
        print(f"Erro ao salvar em {path}: {e}")

# ==============================================================================
# EXECUCAO PRINCIPAL
# ==============================================================================

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # 1. EXTRACAO DOS DADOS.
        df_cli = extract_CSV(spark, PATH_CLIENTES)
        df_ven = extract_TXT(spark, PATH_VENDAS)
        
        # 2. TRANSFORMACAO DOS DADOS.
        df_unificado = join_data(df_cli, df_ven)
        resumo_cli   = get_resumo_clientes(df_unificado)
        balanco_prod = get_balanco_produtos(df_ven)
        
        # 3. CARGA DOS DADOS.
        load_data(resumo_cli, os.path.join(PATH_OUTPUT, "resumo_clientes"), prefixo_arquivo="clientes")
        load_data(balanco_prod, os.path.join(PATH_OUTPUT, "balanco_produtos"), prefixo_arquivo="vendas")
        
        print("\n--- Processamento Finalizado com Sucesso ---")
        resumo_cli.show()
        balanco_prod.show()
    except Exception as e:
        print(f"Erro no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()