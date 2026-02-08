# Projeto PySpark: Consolidação de Vendas e Clientes
Este projeto consiste em desenvolver um pipeline de ETL em PySpark para integrar dados de clientes (CSV) e vendas (TXT Posicional), gerando relatórios de performance e financeiros.

## Cenário e Objetivo
O objetivo é consolidar dados de fontes distintas, realizar o tratamento de valores e entregar saídas estruturadas e particionadas para análise.

## Tecnologias e Pré-requisitos
**Linguagem:** Python 3.x

**Framework:** PySpark 3.x, Pytest 7.x

**Ambiente:** Local (JVM instalada para rodar o Spark)

**Instalação das dependências:**
```
bash
pip install -r requirements.txt
```

## Estrutura do Projeto e Fluxo de Dados
O projeto segue uma arquitetura de "Landing Zone" simples:

**1. Entrada (/data):** Onde os arquivos clientes_*.csv e vendas_*.txt são depositados.

**2. Processamento (main.py):** O script faz a leitura dos arquivos, realiza o Join e aplica as tranformações necessárias para gerar o resumo dos ganhos por cliente e resumo por produto.

**3. Saída (/output):** Arquivos gerados em CSV, particionados por data de venda.

**4. Arquivamento (/processados):** Após o sucessoda execução do script, os arquivos originais são movidos para cá para evitar reprocessamento.

## Como Executar o Pipeline
**1.** Clone o repositório do GitHub para sua maquina local.

**2.** Certifique-se de que os arquivos de entrada seguem o padrão de nome (ex: vendas_20260207.txt).

**3.** Execute o comando:
  ```
  Bash
  python main.py
  ```

## Testes Automatizados

O projeto conta com o uso de testes unitários e de integração utilizando **Pytest** para garantir a integridade das transformações dos dados e do fatiamento posicional feito no arquivo(.txt).

### O que é validado:
**• Extração Posicional:** Valida se o fatiamento das colunas do arquivo **.txt** (Venda, Cliente, Produto, Valor e Data) está seguindo rigorosamente as posições definidas.

**• Conversão de Tipos:** Garante que o campo **valor** é convertido corretamente para decimal (divisão por 100) e o **cliente_id** para inteiro.

**• Lógica de Negócio:** Valida os cálculos de **Soma**, **Contagem** e **Ticket Médio** (arredondado) nos resumos de Clientes e Produtos.

**• Integridade do Join:** Verifica se a união entre a base de Clientes (CSV) e Vendas (TXT) ocorre sem perda de dados.

### Como executar os testes:

**1.** Certifique-se de que as dependências de desenvolvimento estão instaladas:

   ```
   bash
   pip install pytest
   ```

**2.** Execute o comando abaixo na raiz do projeto:

```
Bash
pytest testes_automatizados/test_transformacao.py
```
## Resultado esperado após a execução do teste 
```
=================================================================================== test session starts ====================================================================================
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0
rootdir: (pasta raiz do seu projeto)
collected 1 item

testes_automatizados/test_transformacao.py .                                                                                                                                         [100%]

==================================================================================== 1 passed in 10.26s ====================================================================================
```


## Diferenciais Implementados
Neste projeto, foram aplicadas boas práticas de engenharia além do básico solicitado:

**• Particionamento Dinâmico:** Uso de partitionBy na data de venda para otimizar consultas futuras.

**• Idempotência:** O script pode ser executado múltiplas vezes sem duplicar dados, graças à movimentação dos arquivos processados.

**• Escalabilidade:** O uso da mascara `"_*.csv" e "_*.txt"` na leitura dos arquivos de clientes e vendas permite processar arquivos de vários dias em uma única execução.

## Exemplo de Saída (Output)
Os arquivos são salvos na estrutura de pastas particionadas, segue um exemplo de como os arquivo de saída são gerados.

### Métricas

**Resumo de Clientes:** cliente_id, nome, total_vendas, quantidade_vendas e ticket_medio.

**Balanço de Produtos:** produto_id, total_vendas_produto, quantidade_vendas_produto  ticket_medio_produto.

### Arquivos

**Resumo de Clientes:** `output/resumo_clientes/data_venda=YYYYMMDD/resumo_clientes_YYYYMMDD.csv`

| cliente_id | nome           | total_vendas | quantidade_vendas | ticket_medio |
|:-----------|:---------------|:-------------|:------------------|:-------------|
| 3          | Carlos Andrade | 505.0        | 5                 | 101.0        |
| 1          | Joao Silva     | 870.0        | 6                 | 145.0        |
| 2          | Maria Souza    | 1100.0       | 5                 | 220.0        |
| 4          | Ana Costa      | 670.0        | 4                 | 167.5        |


**Balanço de Produtos:** `output/balanco_produtos/data_venda=YYYYMMDD/balanco_produtos_YYYYMMDD.csv`

| produto_id | total_vendas_produto | quantidade_vendas_produto | ticket_medio_produto |
|:-----------|:---------------------|:--------------------------|:---------------------|
| 00001      | 1470.0               | 8                         | 183.75               |
| 00003      | 875.0                | 5                         | 175.0                |
| 00002      | 800.0                | 7                         | 114.29               |
