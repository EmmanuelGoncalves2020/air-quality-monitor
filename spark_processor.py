from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming import StreamingQuery
import os


# Criar diretórios para resultados se não existirem
os.makedirs('results/processed', exist_ok=True)
os.makedirs('results/alerts', exist_ok=True)
os.makedirs('checkpoints/processed', exist_ok=True)
os.makedirs('checkpoints/alerts', exist_ok=True)

# Inicializar SparkSession
# Nota: No Windows, é importante usar o formato de caminho correto
spark = SparkSession.builder \
    .appName("AirQualityMonitor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoints") \
    .getOrCreate()

# Reduzir nível de log para evitar saída excessiva
spark.sparkContext.setLogLevel("WARN")

# Definir schema para os dados
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("CO2", DoubleType(), True),
    StructField("PM2_5", DoubleType(), True),
    StructField("O3", DoubleType(), True)
])

def process_streaming_data():
    """Processa dados em streaming usando Spark Structured Streaming"""
    print("Iniciando processamento de dados com Spark Structured Streaming...")
    
    # Ler arquivos CSV em streaming
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .csv("data/*.csv")
    
    # Converter timestamp de string para timestamp
    streaming_df = streaming_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Processar dados - calcular médias por estação em janelas de 1 minuto
    processed_df = streaming_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("station_id"),
            col("station_name"),
            col("city"),
            col("latitude"),
            col("longitude")
        ) \
        .agg(
            avg("CO2").alias("avg_CO2"),
            avg("PM2_5").alias("avg_PM2_5"),
            avg("O3").alias("avg_O3")
        ) \
        .withColumn("processing_time", current_timestamp())
    
    # Detectar alertas
    alerts_df = processed_df \
        .filter((col("avg_PM2_5") > 50) | (col("avg_CO2") > 450) | (col("avg_O3") > 80))
    
    # Salvar resultados processados em CSV
    query1 = processed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "results/processed") \
        .option("checkpointLocation", "checkpoints/processed") \
        .start()
    
    # Salvar alertas em CSV
    query2 = alerts_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "results/alerts") \
        .option("checkpointLocation", "checkpoints/alerts") \
        .start()
    
    print("Queries de streaming iniciadas. Pressione Ctrl+C para interromper.")
    
    # Aguardar término das queries
    import time
    while True:
        if query1.isActive or query2.isActive:
            time.sleep(5)
        else:
            break
if __name__ == "__main__":
    process_streaming_data()
