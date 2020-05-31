from kafka.admin import KafkaAdminClient, NewTopic
import sys
import logging
import subprocess
import os


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)



def main():

    topic_name= sys.argv[1]

    try:
        num_partitions= int(sys.argv[2])
    except:
        num_partitions= 1

    try:
        replication_factor= int(sys.argv[3])
    except:
        replication_factor=  1

    try:
        bootstrap_servers=sys.argv[4]
        logging.info('bootstrap_servers: ' + bootstrap_servers)
    except:
        logging.error('bootstrap_servers not specified')

    os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='test')

    topic_list = []

    topic_list.append(NewTopic(name= topic_name , num_partitions=num_partitions, replication_factor=replication_factor))

    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    logging.info('Topic {}, with num_partitions {} and replication_factor {} has been created'.format(topic_name,str(num_partitions),str(replication_factor)))


if __name__ == "__main__":
    main()