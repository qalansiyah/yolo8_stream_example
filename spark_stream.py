from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType 

# Создание сессии Spark и настройка приложения
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Определение схемы для структурированных данных
schema = StructType([
    StructField("frame", StringType())
])

# Чтение данных из Kafka в формате стрима
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "yolo-kafka") \
    .load()

# Преобразование JSON-строк в соответствии с указанной схемой
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Запуск стрима и вывод в консоль
if __name__ == '__main__':
    query = json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()


    query.awaitTermination()
