from kafka import KafkaConsumer
from json import loads
import logging
import sys
import os
from datetime import datetime
import psycopg2
import pymongo

# Getting topic and mode from parameters at run time. If not, throw error.


try:
    topic=sys.argv[1]
    mode=sys.argv[2]
    broker=sys.argv[3]
    logging.info(f'Setting {topic} as topic'.format(topic))
    logging.info(f'Setting {mode} as mode'.format(mode))
    logging.info(f'Setting {broker} as broker'.format(broker))
except IndexError:
    logging.error('-------------------------------------')
    logging.error('You need to specify an existing topic and the mode of execution as an argument at runtime.')
    logging.error('Something like :')
    logging.error('python3 consumer.py topicname stdout/psql/mongo')
    logging.error('-------------------------------------')
    raise


# moving the working directory on folder above

setting_absolute_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

path_to_log_folder= setting_absolute_path + 'logs/' + topic


#check if log directory exist. TRUE: remove everyting inside. FALSE: create the directory


if os.path.exists(path_to_log_folder):
    filelist = [ f for f in os.listdir(path_to_log_folder)]
    for f in filelist:
        os.remove(os.path.join(path_to_log_folder, f))
else:
    os.mkdir(path_to_log_folder)


#logging config depending on mode

if mode=='stdout':
    logging.basicConfig(filename= path_to_log_folder + '/'  +  datetime.now().strftime('%Y%m%d_%H%M%S') +'.log', filemode='w', level=logging.INFO)
else:
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)



###### HELPERS #######

#function that will make code more readble

def connect_to_psql():

    conn=psycopg2.connect(dbname='postgres', user='postgres', host='localhost', password='example', port=4444)
    conn.autocommit = True

    return conn


def close_connection_to_psql(conn):

    conn.close()


#this function will create a table in the docker postgres insatance

def create_table(topic,conn):

    cursor=conn.cursor()

    stmt="""
    DROP TABLE IF EXISTS {0};
    CREATE TABLE {0} (
    uid text,
    message text,
    ts text)
    """.format(topic)

    cursor.execute(stmt)



#this function will insert values in the table table in the docker postgres insatance

def insert_value(table_name, val1 ,val2, val3,conn):


    cursor=conn.cursor()

    stmt="INSERT INTO {} (uid, message, ts) VALUES ('{}', '{}', '{}');".format(table_name,val1,val2,val3)

    cursor.execute(stmt)



###### MAIN FUNCTION #######

#here is where the action takes place

def main():

    consumer = KafkaConsumer(
        topic,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=broker)



    i = 0

    if mode=='psql':

        conn=connect_to_psql()
        create_table(topic,conn)
        close_connection_to_psql(conn)
        logging.info('Table public.{} has been created'.format(topic))
        logging.info('--------------------------------------------')
        logging.info('--------------------------------------------')
        logging.info('Open a new terminal and run : make postgres.')
        logging.info('--------------------------------------------')
        logging.info('--------------------------------------------')
        logging.info('Then run this query: Select count(*) from {};'.format(topic))
        logging.info('--------------------------------------------')
        logging.info('--------------------------------------------')


        conn=connect_to_psql()
        for m in consumer:

            i+=1
            message=str(m.value).replace("'", "''")
            insert_value(topic,
                         str(m.value['uid']),
                         message ,
                         datetime.utcfromtimestamp(int(m.value['ts'])).strftime('%Y-%m-%d %H:%M:%S'),
                         conn)

        close_connection_to_psql(conn)

    elif mode=='stdout':

        for m in consumer:
            i+=1
            print(m.value)

    elif mode=='mongo':

        client = pymongo.MongoClient('mongodb://localhost:27017/',
                                     username='admin',
                                     password='admin')
        logging.info('Connected to Mongo')
        db = client[topic]
        logging.info('Database {} created'.format(topic))
        collection = db[topic]
        logging.info('Collection {} created'.format(topic))

        logging.info('Open a new terminal and run : make mongo.')
        logging.info('--------------------------------------------')
        logging.info('--------------------------------------------')
        logging.info('Then run this command: use {};'.format(topic))
        logging.info('--------------------------------------------')
        logging.info('--------------------------------------------')
        logging.info('Then run this query: db.{}.count();'.format(topic))


        for m in consumer:
            i+=1
            collection.insert(m.value,check_keys=False)



if __name__ == "__main__":
    main()