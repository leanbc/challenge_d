from kafka import KafkaConsumer
from json import loads
import logging
import sys
import os
from datetime import datetime
import psycopg2
import pymongo



# parameters ingestion from Job entry
try:
    topic=sys.argv[1]
    broker=sys.argv[2]
    args={}
    args['topic']=topic
    args['broker']=broker
    logging.info(f'Setting {topic} as topic'.format(topic))
    logging.info(f'Setting {broker} as broker'.format(broker))
except IndexError:
    logging.error('-------------------------------------')
    logging.error('You need to specify an existing topic and the mode of execution as an argument at runtime.')
    logging.error('Something like :')
    logging.error('python3 consumer.py topicname broker')
    logging.error('-------------------------------------')
    raise


def main(args):

    # KafkaConsumer to consume the data
    consumer = KafkaConsumer(
        args['topic'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=args['broker'])

    # Print to stdout messages consumed
    for m in consumer:
        print(m.value)