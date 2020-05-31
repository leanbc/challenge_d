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
import requests



logging.basicConfig(filename= '/logs/'  +  datetime.now().strftime('%Y%m%d_%H%M%S') +'.log', filemode='w', level=logging.INFO)


def main(args):

    #parameters ingestion from Job entry
    try:
        topic = args['topic']
        broker = args['broker']
        window_streaming_seconds= args['window_streaming_seconds']
        logging.info(f'Topic new set to {topic}'.format(topic))
        logging.info((f'Host and port {broker}'.format(broker)))
        logging.info((f'Window Streaming is {window_streaming_seconds} seconds'.format(broker)))
    except IndexError as error:
        logging.error('----------------------------------------------------------------')
        logging.error('You need to specify an existing topic as an argument at runtime.')
        logging.error('Somethingrere like :')
        logging.error('python3 producer.py topicname data_to_stream')
        logging.error('----------------------------------------------------------------')


    #parameters to create the streaming session
    kafkaparams= {
        "bootstrap.servers": broker,
        "auto.offset.reset" : "smallest"
    }

    #adding the jars to the enviroment
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 pyspark-shell'
    
    #creating spark Contecxt 
    sc = SparkContext().getOrCreate()
    ssc = StreamingContext(sc, int(window_streaming_seconds))
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaparams)

    #RDD comming form the stream are transformed in order to calculate some metrics
    #And write them to output path
    try:
        #writing raw data per streamed window
        raw_data = kvs.map(lambda x: x)
        raw_data.saveAsTextFiles( f"/output/{topic}/raw_data/".format(topic))

        #writing count of country per streamed window
        count_of_country = kvs.map(lambda x: json.loads(x[1])).map(lambda x: (x['country'],1))
        count_of_country = count_of_country.reduceByKey(lambda x,y: x + y )
        count_of_country = count_of_country.transform(lambda x: x.sortBy(lambda x: x[1], ascending=False))
        count_of_country.saveAsTextFiles( f"/output/{topic}/count_of_country/".format(topic))

        #writing unique_user count  per streamed window (we assumed one email per user)
        unique_user = kvs.map(lambda x: json.loads(x[1])).map(lambda x: (x['email'],1))
        unique_user = unique_user.reduceByKey(lambda x,y: x + y )
        unique_user = unique_user.map(lambda x:('count_of_unique_users', x[1]))
        unique_user = unique_user.reduceByKey(lambda x,y: x + y )
        unique_user.saveAsTextFiles( f"/output/{topic}/unique_user/".format(topic))

        #writing aggregation of gender count  per streamed window
        gender=kvs.map(lambda x: json.loads(x[1])).map(lambda x: (x['gender'],1))
        gender=gender.reduceByKey(lambda x,y: x + y)
        gender.saveAsTextFiles( f"/output/{topic}/gender_count/".format(topic))

        #using http://ipinfo.io/ to get info from the IP and writing it
        ip_data=kvs.map(lambda x: json.loads(x[1])).map(lambda x: (x['email'],x['ip_address'],requests.get('http://ipinfo.io/{ip_address}'.format(ip_address=x['ip_address'])).json())) 
        ip_data=ip_data.saveAsTextFiles( f"/output/{topic}/ip_data/".format(topic))

        ssc.start()
        ssc.awaitTermination()

    
    except:
        logging.error('somenthing went wrong')

    finally:
        sc.stop()
        logging.info('SparkContext stopped to avoid concurrent context')

            
    

    
