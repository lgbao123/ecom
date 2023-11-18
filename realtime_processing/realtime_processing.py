from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('app.conf')

# kafka config
kafka_bootstrap_server = config.get('kafka','host_name') + ':' + config.get('kafka','port')
input_topic = config.get('kafka','input_topic_name')
# kafka packages
scala_version = '2.12'
spark_version = '3.3.0'
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
    'mysql:mysql-connector-java:5.1.49'
]
# mysqll config
mysql_host = config.get('mysql','host_name') 
mysql_port = config.get('mysql','port')
db_name = config.get('mysql','db_name')

sale_card_type_table = config.get('mysql','sale_card_type_table')
sale_country_table = config.get('mysql','sale_country_table')

db_properties ={
    'user':config.get('mysql','username'),
    'password':config.get('mysql','password'),
    'driver':config.get('mysql','driver'),
}



def save_df_to_mysql( df,batch_id , table_name):
    try :
        df =df.withColumn('batch_no',lit(batch_id))
        mysql_jdbc_url = "jdbc:mysql://" + mysql_host + ":" + str(mysql_port) + "/" + db_name
        #Save the dataframe to the table.
        df.write.jdbc(url = mysql_jdbc_url,
                    table = table_name,
                    mode = 'append',
                    properties = db_properties)
    except Exception as e:
        print(e)
    
if __name__ == '__main__': 
    # get spark context
    spark = SparkSession \
            .builder \
            .appName('ecom')\
            .master('local[*]')\
            .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
    # get input stream
    df = spark.readStream\
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
            .option("subscribe", input_topic) \
            .option("startingOffsets", "latest") \
            .load()
    # define schema 
    schema_df = StructType([
        StructField('order_id', IntegerType()),
        StructField('product_name', StringType()),
        StructField('card_type', StringType()),
        StructField('amount', IntegerType()),
        StructField('order_datetime', TimestampType()),
        StructField('ecommerce_website_name', StringType()),
        StructField('country', StringType()),
        StructField('city', StringType())
    ])
    df = df.selectExpr('CAST(value as string)','timestamp') 
    df =  df.select(from_json('value',schema=schema_df).alias('value'),'timestamp')
    flat_df= df.select('value.*','timestamp')
    flat_df = flat_df.withColumn('partition_date',to_date('order_datetime'))
    flat_df = flat_df.withColumn('partition_hour',hour('order_datetime'))
    # write flat df console
    flat_df.writeStream\
                    .trigger(processingTime='10 seconds')\
                    .format('console')\
                    .option('truncate','false')\
                    .outputMode('update')\
                    .start()
    # write flat df hdfs
    # df_write_hdfs = flat_df.writeStream\
    #                 .trigger(processingTime='10 seconds')\
    #                 .format('parquet')\
    #                 .option('path','/tmp/data/ecom/raw')\
    #                 .option('checkpointLocation','order_df_pre_checkpoint')\
    #                 .outputMode('update')\
    #                 .partitionBy('partition_date','partition_hour')\
    #                 .start()
    # sum total by card_type
    sum_by_card_type_df = flat_df.groupBy('card_type')\
                                    .agg(sum('amount').alias('total_sales'))
    # sum total by country
    sum_by_country_df = flat_df.groupBy('country')\
                                    .agg(sum('amount').alias('total_sales'))
    # write agg df to mysql
    sum_by_card_type_df.writeStream\
                    .trigger(processingTime='10 seconds')\
                    .outputMode('update')\
                    .foreachBatch(lambda df ,id  :save_df_to_mysql(df,id,sale_card_type_table))\
                    .start()
    sum_by_country_df.writeStream\
                    .trigger(processingTime='10 seconds')\
                    .outputMode('update')\
                    .foreachBatch(lambda df ,id  :save_df_to_mysql(df,id,sale_country_table))\
                    .start()
    # write agg df to console
    sum_by_card_type_df.writeStream\
                    .trigger(processingTime='10 seconds')\
                    .format('console')\
                    .option('truncate','false')\
                    .outputMode('update')\
                    .start()
    df_agg_write = sum_by_country_df.writeStream\
                    .trigger(processingTime='10 seconds')\
                    .format('console')\
                    .option('truncate','false')\
                    .outputMode('update')\
                    .start()
    
    df_agg_write.awaitTermination()


 
    print("Real-Time Data Processing Application Completed.")

        
