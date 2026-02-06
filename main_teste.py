from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum, count, avg, round
import os
import shutil

# ==============================================================================
# PARAMETRIZACAO DE VARIAVEIS CRIACAO DE SESSAO SPARK
# ==============================================================================
# 1. Descobre o caminho da pasta onde este script (.py) está salvo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 2. Monta os caminhos baseando-se na estrutura de pastas do projeto
# Isso evita o uso de /home/berto/... ou do til (~)
PATH_CLIENTES = os.path.join(BASE_DIR, "data", "clientes.csv")
PATH_VENDAS   = os.path.join(BASE_DIR, "data", "vendas.txt")
PATH_OUTPUT   = os.path.join(BASE_DIR, "output")
DELIMITADOR_CSV = "," 

def create_spark_session():
    return SparkSession.builder \
        .appName("Projeto_ETL_Spark") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

# ==============================================================================
# LEITURA DOS ARQUIVOS DE CLIENTES E VENDAS
# ==============================================================================

# 1. EXTRACAO DOS DADOS DO ARQUIVO CSV DE CLIENTES
def extract_CSV(spark, clientes_path):
    print(f"Lendo arquivo de: {clientes_path}")
    return spark.read.csv(clientes_path, header=True, inferSchema=True, sep=DELIMITADOR_CSV)

# 2. ESTRACAO DOS DADOS DO ARQUIVO TXT DE VENDAS
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
# TRANSFORMACAO DOS DADOS DE CLIENTE E VENDAS
# ==============================================================================
def transform_data(df_clientes, df_vendas):
    print(f"Realizando a transformação dos dados.")
    df_joined = df_vendas.join(df_clientes, on="cliente_id", how="inner")
    
    # 1. RESUMO POR CLIENTE
    resumo_clientes = df_joined.groupBy("cliente_id", "nome", "data_venda").agg(
        round(sum("valor"), 2).alias("total_vendas"),
        count("venda_id").alias("quantidade_vendas"),
        round(avg("valor"), 2).alias("ticket_medio")
    )
    
    # 2. BALANCO POR PRODUTO
    balanco_produtos = df_vendas.groupBy("produto_id", "data_venda").agg(
        round(sum("valor"), 2).alias("total_vendas_produto"),
        count("venda_id").alias("quantidade_vendas_produto"),
        round(avg("valor"), 2).alias("ticket_medio_produto")
    )
    
    return resumo_clientes, balanco_produtos

# ==============================================================================
# CRIACAO DOS DIRETORIOS, PARTICOES E ARQUIVOS
# ==============================================================================
def load_data(df, path, prefixo_arquivo, partition_col="data_venda"):
    try:
        # 1. Salva usando repartition(1) para garantir um único arquivo por pasta de partição
        df.repartition(1).write.mode("overwrite") \
            .option("header", "true") \
            .partitionBy(partition_col) \
            .csv(path)
        
        # 2. Percorre as pastas para renomear os arquivos e limpar metadados
        for root, dirs, files in os.walk(path):
            for file in files:
                # Se for o arquivo de dados gerado pelo Spark
                if file.endswith(".csv") and file.startswith("part-"):
                    # Captura o valor da data da pasta atual (ex: data_venda=20230401)
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
        # ETL - Extração e Transformação
        df_cli = extract_CSV(spark, PATH_CLIENTES)
        df_ven = extract_TXT(spark, PATH_VENDAS)
        resumo_cli, balanco_prod = transform_data(df_cli, df_ven)
        
        # Load - Salvando com nomes específicos
        load_data(resumo_cli, 
                  os.path.join(PATH_OUTPUT, "resumo_clientes"), 
                  prefixo_arquivo="clientes")
        
        load_data(balanco_prod, 
                  os.path.join(PATH_OUTPUT, "balanco_produtos"), 
                  prefixo_arquivo="vendas")
        
        print("\n--- Processamento Finalizado com Sucesso ---")
        resumo_cli.show()
        balanco_prod.show()
    except Exception as e:
        print(f"Erro no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()