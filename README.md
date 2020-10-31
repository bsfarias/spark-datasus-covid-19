# DATASUS COVID19

## Introdução

Projeto para capturar e armazenar os dados de covid-19 disponibilizados pelo DataSus.

##  Descrição

Os dados são extraídos diretamente do site [dados.gov.br/](https://dados.gov.br/dataset/bd-srag-2020).

Utiliza-se um Job Spark para fazer a busca de um arquivo csv que, após processamento, é transformado em um dataframe e persistido no S3 em formato parquet.

## Execução do job spark:
```
 spark-submit --master local[*] extract_datasus_covid19.py <Bucket name> <Aws Access Key> <Aws Secret Key>
```