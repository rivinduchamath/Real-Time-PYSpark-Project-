from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "orderstopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
kafka_data_file_path = "/dddddddd/sss.csv"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "root"
mysql_password = "mynewpassword"
mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_kwyspace_name = "sales_ks"
cassandra_table_name = "order_tbl"


def save_to_cassandra(current_df, epoc_id):
    print("Printing epoc_id: ")
    print(epoc_id)

    print("Printing before Cassandra table save : ", str(epoc_id))
    current_df \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="DataSciTable", keyspace="DataSci") \
        .save()
    print("Printing before Cassandra table save : ", str(epoc_id))


def save_to_mysql(current_df, epoc_id):
    db_credentials = {"user": mysql_user_name,
                      "password": mysql_password,
                      "driver": mysql_driver_class}
    print("Printing epoc_id: ")
    print(epoc_id)
    print(current_df.printSchema())
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final = current_df \
        .withColumn("processed_at", lit(processed_at)) \
        .withColumn("batch_it", lit(epoc_id))
    print(current_df_final.printSchema())
    print("Printing before Msql table save: " + str(epoc_id))
    current_df_final.write \
        .jbdc(url=mysql_jdbc_url,
              table=mysql_table_name,
              mode="append",
              properties=db_credentials)
    print("Printing after Msql table save: " + str(epoc_id))


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    print("Data Processing Application Started...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars",
                "file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.0"
                ".jar,file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraClassPath",
                "file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.0.jar"
                ":file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraLibrary",
                "file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.0.jar"
                ":file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar") \
        .config("spark.driver.extraClassPath",
                "file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.0.jar"
                ":file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar") \
        .config("spark.cassandra.connection.host", cassandra_host_name) \
        .config("spark.cassandra.connection.port", cassandra_port_no) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("created_at", StringType()) \
        .add("discount", StringType()) \
        .add("product_id", StringType()) \
        .add("quantity", StringType()) \
        .add("subtotal", StringType()) \
        .add("tax", StringType()) \
        .add("total", StringType()) \
        .add("customer_id", StringType())

    orders_df2 = orders_df1 \
        .select(from_json(col("value"), orders_schema).alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")

    orders_df3 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .foreachBatch(save_to_cassandra) \
        .start()

    customers_df = spark.read.csv(kafka_data_file_path, header=True, inferSchema=True)
    customers_df.printSchema()
    customers_df.show(5, False)

    orders_df4 = orders_df3.join(customers_df, orders_df3.customer_id == customers_df.customer_id, how='inner')
    print("Printing Schema of orders_df4 ")
    orders_df4.printSchema()

    orders_df5 = orders_df4.groupBy("source", "state") \
        .agg({'total': 'sum'}).select("source", "state", col("sum(total)").alias("total_sum_amount"))

    print("Printing Schema of orders_df5: ")
    orders_df5.printSchema()

    # Write final result into console for debugging purpose

    trans_detail_write_stream = orders_df5 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    orders_df5 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .foreachBatch(save_to_mysql) \
        .start()

    trans_detail_write_stream.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")
