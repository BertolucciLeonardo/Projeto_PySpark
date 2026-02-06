from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum, count, avg, round
import os

# ==============================================================================
# PARAMETRIZACAO DE VARIAVEIS LEITURA DE ARQUIVOS
# ==============================================================================
PATH_CLIENTES = "/home/berto/alura/aula_spark/projeto_spark/data/clientes.csv"
PATH_VENDAS = "/home/berto/alura/aula_spark/projeto_spark/data/vendas.txt"
PATH_OUTPUT = "/home/berto/alura/aula_spark/projeto_spark/output"
DELIMITADOR_CSV = ","  # Altere para "," ou ";" conforme a necessidade

def create_spark_session():
    return SparkSession.builder \
        .appName("Projeto_ETL_Spark") \
        .getOrCreate()

# EXTRACAO E PREPARACAO DOS DADOS
def extract_data(spark, clientes_path, vendas_path):
    print(f"Lendo arquivos de: {clientes_path} e {vendas_path}")
    
    # 1. Leitura do CSV de Clientes
    df_clientes = spark.read.csv(
    clientes_path, 
    header=True, 
    inferSchema=True, 
    sep=DELIMITADOR_CSV
)
    
    # 2. Leitura do TXT Posicional de Vendas
    raw_vendas = spark.read.text(vendas_path)
    
    # Fatiamento posicional conforme o PDF
    df_vendas = raw_vendas.select(
        substring(col("value"), 1, 5).alias("venda_id"),
        substring(col("value"), 6, 5).cast("int").alias("cliente_id"), # Cast para Join
        substring(col("value"), 11, 5).alias("produto_id"),
        (substring(col("value"), 16, 8).cast("double") / 100).alias("valor"),
        substring(col("value"), 24, 8).alias("data_venda")
    )
    return df_clientes, df_vendas

# TRANSFORMACAO DOS DADOS
def transform_data(df_clientes, df_vendas):

    # Join entre cliente e vendas
    df_joined = df_vendas.join(df_clientes, on="cliente_id", how="inner")
    
    # 1. Resumo por Cliente
    resumo_clientes = df_joined.groupBy("cliente_id", "nome").agg(
        round(sum("valor"), 2).alias("total_vendas"),
        count("venda_id").alias("quantidade_vendas"),
        round(avg("valor"), 2).alias("ticket_medio")
    )
    
    # 2. Balanço por Produto
    balanco_produtos = df_vendas.groupBy("produto_id").agg(
        round(sum("valor"), 2).alias("total_vendas_produto"),
        count("venda_id").alias("quantidade_vendas_produto"),
        round(avg("valor"), 2).alias("ticket_medio_produto")
    )
    
    return resumo_clientes, balanco_produtos

# Salvando CSV (leitura fácil)
def load_data(df, path, partition_col=None):
    try:
        
        writer = df.coalesce(1).write.mode("overwrite").option("header", "true")
        
        if partition_col:
            writer.partitionBy(partition_col).csv(path)
        else:
            writer.csv(path)
            
        print(f"Sucesso: Dados salvos em {path}")
    except Exception as e:
        print(f"Erro ao salvar: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR") # Limpa o console de logs desnecessários
    
    try:
        # ETL
        df_cli, df_ven = extract_data(spark, PATH_CLIENTES, PATH_VENDAS)
        resumo_cli, balanco_prod = transform_data(df_cli, df_ven)
        
        # Load
        load_data(resumo_cli, os.path.join(PATH_OUTPUT, "resumo_clientes"))
        load_data(balanco_prod, os.path.join(PATH_OUTPUT, "balanco_produtos"))
        
        print("\n--- Processamento Finalizado com Sucesso ---")
        resumo_cli.show()
        balanco_prod.show()
        
    except Exception as e:
        print(f"Erro no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()