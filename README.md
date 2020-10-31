# DATASUS COVID19

## Introdução

Projeto para capturar e armazenar os dados de covid-19 disponibilizados pelo DataSus.

##  Descrição

Os dados são extraídos diretamente do site [dados.gov.br/](https://dados.gov.br/dataset/bd-srag-2020).

Utiliza-se um Job Spark para fazer a busca de um arquivo csv que, após processamento, é transformado em um dataframe e persistido no S3 em formato parquet.

## Construção do ambiente através do docker compose:
   - Certifique-se de que o [docker](https://www.docker.com/products/docker-desktop) esteja instalado em sua máquina e execute o seguinte comando:
```
   cd datasus-covid-19/docker/
   docker-compose up
```   

## Execução do job spark:
   - Acesse o container e faça o submit do job
```
   docker exec -i -t spark /bin/bash
   spark-submit --master local[*] --packages  org.apache.hadoop:hadoop-aws:3.1.0 /home/jovyan/scripts/extract_datasus_covid19.py <Bucket name> <Aws Access Key> <Aws Secret Key>
```