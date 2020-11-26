# SPARK DATASUS COVID19

## Introdução

Projeto para capturar e armazenar os dados de covid-19 disponibilizados pelo DataSus. Os dados podem ser obtidos através do site [dados.gov.br/](https://dados.gov.br/dataset/bd-srag-2020).
Utiliza-se um Job Spark para fazer a busca de um arquivo csv que, após processamento, é transformado em um dataframe e persistido no storage local em formato parquet.

## Pré-requisitos:
* [docker](https://www.docker.com/products/docker-desktop)

## Construção do ambiente através do docker compose:
   - Na pasta raiz do projeto, execute o seguinte comando:
```
docker-compose up
```   

## Execução do job spark:
   - Acesse o container e faça o submit do job
```
   docker exec -i -t spark /bin/bash

   spark-submit --master local[*] /home/jovyan/scripts/extract_datasus_covid19.py --target_path /home/jovyan/work/data/raw/datasus/covid19/
```