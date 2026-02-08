# Projeto ETL PySpark: Consolidação de Vendas e Clientes
Este projeto é a resolução de um desafio técnico para Engenharia de Dados. Ele consiste em um pipeline de ETL desenvolvido em PySpark para integrar dados de clientes (CSV) e vendas (TXT Posicional), gerando relatórios de performance e financeiros.

## Cenário e Objetivo
O objetivo é consolidar dados de fontes distintas, realizar o tratamento de valores decimais em arquivos posicionais e entregar saídas estruturadas e particionadas para análise.

## Tecnologias e Pré-requisitos
**Linguagem:** Python 3.x

**Framework:** PySpark 3.x

**Ambiente:** Local (JVM instalada para rodar o Spark)

**Instalação das dependências:**
```
bash
pip install -r requirements.txt
```

## Estrutura do Projeto e Fluxo de Dados
O projeto segue uma arquitetura de "Landing Zone" simples:

**1. Entrada (/data):** Onde os arquivos clientes_*.csv e vendas_*.txt são depositados.

**2. Processamento:** O script lê os arquivos, realiza o Join, calcula Ticket Médio e soma de vendas.

**3. Saída (/output):** Arquivos gerados em CSV, particionados por data de venda.

**4. Arquivamento (/processados):** Após o sucesso, os arquivos originais são movidos para cá para evitar reprocessamento.

## Como Executar o Pipeline
**1.** Clone o repositório.

**2.** Certifique-se de que os arquivos de entrada seguem o padrão de nome (ex: vendas_20260207.txt).

**3.** Execute o comando:
  ```
  Bash
  python main.py
  ```

## Diferenciais Implementados
Neste projeto, foram aplicadas boas práticas de engenharia além do básico solicitado:

**• Particionamento Dinâmico:** Uso de partitionBy na data de venda para otimizar consultas futuras.

**• Idempotência:** O script pode ser executado múltiplas vezes sem duplicar dados, graças à movimentação dos arquivos processados.

**• Escalabilidade:** O uso de curingas (*) permite processar arquivos de vários dias em uma única execução.

## Exemplo de Saída (Output)
Os arquivos são salvos na estrutura de pastas particionadas, segue um exemplo de como os arquivo de saída são gerados.

### Métricas

**Resumo de Clientes:** cliente_id, nome, total_vendas, quantidade_vendas e ticket_medio.

**Balanço de Produtos:** produto_id, total_vendas_produto, quantidade_vendas_produto  ticket_medio_produto.

### Arquivos

**Resumo de Clientes:** output/resumo_clientes/data_venda=YYYYMMDD/resumo_clientes_YYYYMMDD.csv

**Balanço de Produtos:** output/balanco_produtos/data_venda=YYYYMMDD/balanco_produtos_YYYYMMDD.csv