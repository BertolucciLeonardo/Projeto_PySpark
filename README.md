🚀 Desafio ETL PySpark: Consolidação de Vendas e Clientes
Este projeto é a resolução de um desafio técnico para Engenharia de Dados. Ele consiste em um pipeline de ETL desenvolvido em PySpark para integrar dados de clientes (CSV) e vendas (TXT Posicional), gerando relatórios de performance e financeiros.

📋 Cenário e Objetivo
O objetivo é consolidar dados de fontes distintas, realizar o tratamento de valores decimais em arquivos posicionais e entregar saídas estruturadas e particionadas para análise.

🛠️ Tecnologias e Pré-requisitos
Linguagem: Python 3.x

Framework: PySpark 3.x

Ambiente: Local (JVM instalada para rodar o Spark)

Instalação das dependências:

Bash
pip install -r requirements.txt
📂 Estrutura do Projeto e Fluxo de Dados
O projeto segue uma arquitetura de "Landing Zone" simples:

Entrada (/data): Onde os arquivos clientes_*.csv e vendas_*.txt são depositados.

Processamento: O script lê os arquivos, realiza o Join, calcula Ticket Médio e soma de vendas.

Saída (/output): Arquivos gerados em CSV, particionados por data de venda.

Arquivamento (/processados): Após o sucesso, os arquivos originais são movidos para cá para evitar reprocessamento.

⚙️ Como Executar o Pipeline
Clone o repositório.

Certifique-se de que os arquivos de entrada seguem o padrão de nome (ex: vendas_20260207.txt).

Execute o comando:

Bash
python main.py
💎 Diferenciais Implementados
Neste projeto, foram aplicadas boas práticas de engenharia além do básico solicitado:

Particionamento Dinâmico: Uso de partitionBy na data de venda para otimizar consultas futuras.

Idempotência: O script pode ser executado múltiplas vezes sem duplicar dados, graças à movimentação dos arquivos processados.

Escalabilidade: O uso de curingas (*) permite processar arquivos de vários dias em uma única execução.

📊 Exemplo de Saída (Output)
Os arquivos são salvos na estrutura de pastas particionadas, facilitando a leitura por ferramentas de BI ou outros processos de Big Data.

Resumo de Clientes
output/resumo_clientes/data_venda=YYYYMMDD/clientes_YYYYMMDD.csv

Balanço de Produtos
output/balanco_produtos/data_venda=YYYYMMDD/produtos_YYYYMMDD.csv