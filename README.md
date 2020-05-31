Kafka-Docker-Solution
========================

## Minimum requirements

* Docker 19.03.5
* Python3

# Description

### Infraestructure

Consists of 3 docker containers each of them encapsulating the following services

* Kafka version 2.4.0
* Zookeeper 3.4.6
* Postgres 12.1
* Mongo 4.2.2 

For this MVP, only a one single broker will be spun up.

### Logic of the workflow

- Spin up cluster 
- Create Kafka Topic
- Create Kafka Producer that will download a jsonl file from `'http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz'`
- Create Kafka Consumer that will consume the jsonl file
- Count Consumed Messages 

### How to achive this logic

There are 2 ways of handling this cluster:

- Via Command Line / Terminal
- Via Make commands (Make is required in this case)

Both ways will be explained parallelly

### Let's get started

##### Change to the Repo Directory


- cd to `data-challenge-eng` directory.

##### Install python modules 

- run : `pip3 install -r requirements.txt`

##### Spin up cluster 
- `docker-compose up -d` OR `make start_cluster`

 Once the 4 instances are running it is time to:
 
##### Create topic

- `python3 python_scripts/create_topic.py name_of_the_topic number_of_partitions replication_factor`
    - for example: `python3 python_scripts/create_topic.py test 1 1`
    
- `make create_topic TOPIC=name_of_the_topic`
    - for example: `make create_topic TOPIC=test`
  
##### Create Producer

- `python3 python_scripts/producer.py name_of_the_topic_you_want_to_send`
    - for example: `python3 python_scripts/producer.py test`
    
- `make create_producer TOPIC=name_of_the_topic`
    - for example: `make create_producer TOPIC=test`

The producer will download the file from `'http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz'` send the messages to the desired topic.

The logs on the console will let you know that the producer is working:
```INFO:Producer is reading the file now```

##### Create Consumer

The consumer can be set in 3 modes: `stdout` or `psql` or `mongo`

    - `stdout`: will output messages to console and create  a simple count in the logs directory.
    - `psql`: will create a table, with the name of the topic, in the postgres instance.
    - `mongo`: will create a database, with the name of the topic, in the mongo instance.

- `python3 python_scripts/consumer.py.py name_of_the_topic_you_want_to_read mode`
    - for example: `python3 python_scripts/consumer.py.py test psql`
    - for example: `python3 python_scripts/consumer.py.py test stdout`
    - for example: `python3 python_scripts/consumer.py.py test mongo`
    
- `make create_consumer TOPIC=yourtopicname MODE=mode`
    - for example: `make create_consumer TOPIC=test MODE=stdout`
    - for example: `make create_consumer TOPIC=test MODE=psql`
    - for example: `make create_consumer TOPIC=test MODE=mongo`

##### count messages of the last consumer created

- If consumer mode = `stdout` , run this from the command line:

    -Run `sh count.sh name_of_the_topic` the count of messages will be printed to the console


- If consumer mode = `psql`:

    - Via Make run: `make postgres`. Which will log you in the psql container.
    - Via Docker: `docker exec -it data-challenge-eng_db_1 psql -U postgres `
   
    And then run: `Select count(*) From topic_name;` it will give you the number of mesagges inserted at the moment.

- If consumer mode = `mongo`:

    - Via Make run: `make mongo`. Which will log you in the mongo container.
    - Via Docker: `docker exec -it mongodb  mongo --username admin --password admin `
   
    And then run: `use yourtopicname;` and after that `db.yourtopicname.count();`


##### Close and Destroy The Cluster

run: `make stop_cluster`

