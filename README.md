# Projeto de Montagem e Processamento de Dados com Databricks

Este projeto utiliza o Databricks para montar um bucket S3 da AWS, criar diretórios no DBFS, processar dados em diferentes camadas (Bronze, Silver e Gold) e criar tabelas dimensionais e de fato utilizando Delta Lake.

## Objetivo

O objetivo deste projeto é demonstrar como utilizar o Databricks para realizar o processamento de dados em um ambiente de Data Lake, desde a ingestão dos dados brutos até a criação de tabelas otimizadas para análise.

## Estrutura do Projeto

1. **Montagem do Bucket S3**
    - Montagem de um bucket S3 no DBFS utilizando chaves de acesso da AWS para armazenar os dados brutos.

2. **Criação de Diretórios no DBFS**
    - Criação de diretórios no DBFS para organizar os dados em diferentes zonas de processamento: `landingzone`, `bronze`, `silver` e `gold`.

3. **Processamento de Dados**
    - Leitura dos dados brutos da camada `landingzone`, processamento e salvamento dos dados na camada `bronze`.
    - Transformação dos dados na camada `silver` para padronização e limpeza.
    - Criação de tabelas dimensionais e de fato na camada `gold` utilizando Delta Lake para otimização e análise.

4. **Carregamento Incremental**
    - Implementação de carregamento incremental para atualizar as tabelas na camada `gold` com novos dados.

5. **Criação de Banco de Dados e Tabelas no Databricks**
    - Criação de um banco de dados no Databricks e definição das tabelas utilizando os dados armazenados na camada `gold`.

## Tecnologias Utilizadas

- **Databricks**: Plataforma de dados baseada em Apache Spark.
- **AWS S3**: Serviço de armazenamento de objetos da Amazon Web Services.
- **Delta Lake**: Camada de armazenamento que traz confiabilidade aos Data Lakes.