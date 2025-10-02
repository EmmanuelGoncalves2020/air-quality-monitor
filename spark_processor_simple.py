from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd
import glob
import os
import time

# Criar diretórios para resultados se não existirem
os.makedirs('results/processed', exist_ok=True)
os.makedirs('results/alerts', exist_ok=True)

# Inicializar SparkSession
spark = SparkSession.builder\
    .appName("AirQualityMonitorSimple")\
    .config("spark.sql.shuffle.partitions", "2")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def process_batch_data():
    """Processa dados em lotes usando Spark"""
    print("Iniciando processamento de dados em lotes com Spark...")
    
    while True:
        try:
            # Encontrar arquivos CSV na pasta data
            csv_files = glob.glob('data/*.csv')
            
            if not csv_files:
                print("Nenhum arquivo encontrado. Aguardando...")
                time.sleep(5)
                continue
            
            # Processar o arquivo mais recente
            latest_file = max(csv_files, key=os.path.getmtime)
            print(f"Processando arquivo: {latest_file}")
            
            # Ler o arquivo CSV com Spark
            df = spark.read.option("header", "true").csv(latest_file)
            
            # Converter tipos de dados
            df = df.withColumn("CO2", col("CO2").cast(DoubleType())) \
                   .withColumn("PM2_5", col("PM2_5").cast(DoubleType())) \
                   .withColumn("O3", col("O3").cast(DoubleType()))
            
            # Calcular médias por estação
            processed_df = df.groupBy("station_id", "station_name", "city", "latitude", "longitude") \
                            .agg(
                                avg("CO2").alias("avg_CO2"),
                                avg("PM2_5").alias("avg_PM2_5"),
                                avg("O3").alias("avg_O3")
                            ) \
                            .withColumn("processing_time", current_timestamp())
            
            # Detectar alertas
            alerts_df = processed_df.filter(
                (col("avg_PM2_5") > 50) | (col("avg_CO2") > 450) | (col("avg_O3") > 80)
            )
            
            # Salvar resultados
            timestamp = int(time.time())
            
            # Salvar dados processados
            processed_file = f"results/processed/processed_{timestamp}.csv"
            processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(processed_file)
            print(f"Dados processados salvos em: {processed_file}")
            
            # Salvar alertas se houver
            if alerts_df.count() > 0:
                alerts_file = f"results/alerts/alerts_{timestamp}.csv"
                alerts_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(alerts_file)
                print(f"Alertas salvos em: {alerts_file}")
            
            # Aguardar antes do próximo processamento
            time.sleep(10)
            
        except Exception as e:
            print(f"Erro no processamento: {e}")
            time.sleep(5)

if __name__ == "__main__":
    process_batch_data()
