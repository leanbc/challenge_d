from kafka import KafkaProducer
import json
import sys
import urllib.request
import gzip
import shutil
import logging
import os
import time


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# Getting topic and mode from parameters at run time. If not, throw error.
try:
    topic=sys.argv[1]
    data_to_produce=sys.argv[2]
    broker=sys.argv[3]
    logging.info(f'Topic new set to {topic}'.format(topic))
    logging.info((f'Data willl be produced from {data_to_produce}'.format(data_to_produce)))
    logging.info((f'Host and port {broker}'.format(broker)))
except IndexError as error:
    logging.error('----------------------------------------------------------------')
    logging.error('You need to specify an existing topic as an argument at runtime.')
    logging.error('Somethingrere like :')
    logging.error('python3 producer.py topicname data_to_stream')
    logging.error('----------------------------------------------------------------')

# moving the working directory on folder above

absolute_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

###### MAIN FUNCTION #######

#here is where the action takes place

def main():

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

    #reading file

    # with open(setting_absolute_path + '/data_to_load/' + data_to_produce, 'r') as json_file:
    with open(absolute_path +'data_to_load/' + data_to_produce) as json_file:
        json_list = json.load(json_file)

    logging.info('Producer is reading the file now: /Users/leandro.bleda-cantos/challenge/data-challenge-eng/data_to_load/mock_data.json')

    #sending file to topic
    for json_item in json_list:
        try:
            result = json_item
            producer.send(topic, value=result)
            producer.flush()
        except:
            result = dict({'error': 'Not Json Object'})
        finally:
            time.sleep(2)

if __name__ == "__main__":
    main()