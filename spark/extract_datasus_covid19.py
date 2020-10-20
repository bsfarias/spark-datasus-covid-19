from pyspark.sql import SparkSession
import requests

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


    return spark.read \
                .option("delimiter",";") \
                .option("header",True) \
                .option("inferSchema", "true") \
                .csv(path)

def get_covid19_data(df):
    """
    Seleciona apenas os dados de covid-19
            Parametros:
                    df (obj): DataFrame com os dados de srag.
            Retorno:
                    df (obj): DataFrame com dados de covid-19 (CLASSI_FIN=5).
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

def main():
    """
    Função principal.
            Retorno: None
    """

    spark = SparkSession.builder.appName("extract_datasus_covid19").getOrCreate()

    df_raw = download_srag_data(spark)
    
    df_covid = get_covid19_data(df_raw)
    
    df_covid \
      .coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("compression", "gzip") \
      .format("parquet") \
      .save("/home/jovyan/work/covid19")

 if __name__ == "__main__":
    main()