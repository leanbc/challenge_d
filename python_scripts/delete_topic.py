from kafka.admin import KafkaAdminClient, NewTopic
import sys
import logging


def main():

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    topic_name= []

    topic_name.append(sys.argv[1])

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

    admin_client.delete_topics(topics=topic_name)

    logging.info('Topic {}, has been deleted'.format(topic_name))

if __name__ == "__main__":
    main()