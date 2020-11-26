import requests
import sys
from pyspark.sql import SparkSession
from argparse import ArgumentParser

def create_spark_session():
    """ Retorna uma sessão spark
    Retorno:
        SparkSession: sessao spark
    """
    return SparkSession.builder.appName("extract_datasus_covid19").getOrCreate()

def download_srag_data (spark):
    """
    Baixa os dados de Síndrome Respiratória Aguda Grave (srag) - incluindo dados da COVID-19
            Parametros:
                    spark (obj) : Sessão spark
            Retorno:
                    df (obj): DataFrame de srag.
    """
    path = '/home/jovyan/work/srag_data.csv'
    url = 'https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2020/INFLUD-28-09-2020.csv'
    req = requests.get(url)
    url_content = req.content
    csv_file = open(path, 'wb')
    csv_file.write(url_content)
    csv_file.close()
    return spark.read \
                .option("delimiter",";") \
                .option("header",True) \
                .option("inferSchema", "true") \
                .csv(path)

def get_covid19_data(df):
    """ Seleciona apenas os dados de covid-19
    Parametros:
            df: DataFrame com os dados de srag.
    Retorno:
            df: DataFrame com dados de covid-19 (CLASSI_FIN=5).
    """
    df_covid = df.select('SG_UF_NOT'
                        ,'CS_SEXO'
                        ,'NU_IDADE_N'
                        ,'CS_GESTANT'
                        ,'CS_RACA'
                        ,'NOSOCOMIAL'
                        ,'FEBRE'
                        ,'TOSSE'
                        ,'GARGANTA'
                        ,'DISPNEIA'
                        ,'DESC_RESP'
                        ,'SATURACAO'
                        ,'DIARREIA'
                        ,'VOMITO'
                        ,'DOR_ABD'
                        ,'FADIGA'
                        ,'PERD_OLFT'
                        ,'PERD_PALA'
                        ,'PUERPERA'
                        ,'FATOR_RISC'
                        ,'CARDIOPATI'
                        ,'HEMATOLOGI'
                        ,'SIND_DOWN'
                        ,'HEPATICA'
                        ,'ASMA'
                        ,'DIABETES'
                        ,'NEUROLOGIC'
                        ,'PNEUMOPATI'
                        ,'IMUNODEPRE'
                        ,'RENAL'
                        ,'OBESIDADE'
                        ,'VACINA'
                        ,'ANTIVIRAL'
                        ,'EVOLUCAO') \
                .where("CLASSI_FIN=5")
    return df_covid

def load_df(df, target_path):
    """ Persiste um dataframe em um storage
    Parametros:
            df: DataFrame com os dados de srag.
    Retorno:
            None
    """
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .format("parquet") \
      .save(target_path)

def main():
    """ Função principal.
    """
    parser = ArgumentParser()
    parser.add_argument('--target_path', help='Caminho no qual o arquivo parquet final será gravado', required=True)
    args = parser.parse_args()

    spark = create_spark_session()

    load_df(get_covid19_data(download_srag_data(spark)), args.target_path)
     #"/home/jovyan/work/data/raw/datasus/covid19/"

if __name__ == "__main__":
        main()