import os
import sys
import findspark
if os.path.basename(os.getcwd())=='notebook':
    os.chdir(os.path.dirname(os.getcwd()))
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
findspark.init()
from pyspark.sql import SparkSession 
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import os
import logging
import json
from datetime import datetime


def main(args):


    #parameters ingestion from Job entry
    try:
        topic = args['topic']
        broker = args['broker']
        output_format= args['output_format']
        window_streaming_seconds= args['window_streaming_seconds']
        logging.info(f'New Topic set to {topic}'.format(topic))
        logging.info((f'Host and port {broker}'.format(broker)))
    except IndexError as error:
        logging.error('----------------------------------------------------------------')
        logging.error('You need to specify an existing topic as an argument at runtime.')
        logging.error('Somethingrere like :')
        logging.error('python3 producer.py topicname data_to_stream')
        logging.error('----------------------------------------------------------------')

    #adding the jars to the enviroment
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell'

    #creating park session
    spark = SparkSession \
            .builder \
            .appName("PythonStreamingReciever") \
            .master("local") \
            .getOrCreate()


    try:
        #Reading the stream from Kafka 
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", broker) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() 

        #formatting the message form Kafka    
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # writing Stream in the format selected in the parameters and to the location /output/topicname
        ds = df.writeStream \
            .format(output_format) \
            .option("format", "append") \
            .trigger(processingTime = f"{window_streaming_seconds} seconds".format(window_streaming_seconds=str(window_streaming_seconds))) \
            .option("path", f"/output/{topic}/raw_data_df".format(topic)) \
            .option("checkpointLocation", f"/output/{topic}/checkpointLocation".format(topic)) \
            .outputMode("append") \
            .start()

        ds.awaitTermination()

    except:
        logging.error('somenthing went wrong')

    finally:
        spark.stop()
        logging.error('SparkContext stopped to avoid concurrent context')