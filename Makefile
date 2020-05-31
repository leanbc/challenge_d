SHELL:=/bin/bash

start_cluster : _start_cluster

postgres : _postgres

stop_cluster : _stop_cluster

create_topic : _create_topic


# Run it like: make create_topic TOPIC=test PARITITIONS=1 REPLICATION=1 BROKER=kafka:9093


create_consumer : _create_consumer

# Run it like: make create_consumer TOPIC=test MODE=stdout
# OR
# Run it like: make create_consumer TOPIC=test MODE=psql

create_producer : _create_producer

# Run it like: make create_producer TOPIC=test DATA_TO_PRODUCE=mock_data

mongo: _mongo

# Run it like: make mongo


CURRENT_DIR_NAME:=`pwd | xargs basename`
DEV_DOCKER_NAME=spark_run
DEV_IMAGE_ID=sebnyberg/pyspark-alpine:latest


_start_cluster:
	( \
	docker-compose up -d ; \
	)


_postgres:
	( \
	 docker exec -it data-challenge-eng_db_1 psql -U postgres ; \
	)

_stop_cluster:
	( \
	 docker-compose stop ; \
	 docker-compose rm -f; \
	)

_create_topic:
	( \
	 python3 ./python_scripts/create_topic.py ${TOPIC} ${PARITITIONS} ${REPLICATION} ${BROKER} ; \
	)

_create_consumer:
	( \
	 python3 ./python_scripts/consumer.py ${TOPIC} ${MODE} ; \
	)

_create_producer:
	( \
	 python3 ./python_scripts/producer.py ${TOPIC} ${DATA_TO_PRODUCE} ${BROKER}; \
	)

_mongo:
	( \
	docker exec -it mongodb  mongo --username admin --password admin ;\
	)

_pyspark:
	( \
	docker exec -it \
	spark-enviroment spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5  /python_scripts/stream_spark.py; \
	)

_create_topic_docker:
	( \
	docker exec -it \
	spark-enviroment \
	bash -c "pip install -r /requirements.txt && python3 /python_scripts/create_topic.py ${TOPIC} ${PARITITIONS} ${REPLICATION} ${BROKER}";\
	)
# Run it like: make _create_topic_docker TOPIC=test1 PARITITIONS=1 REPLICATION=1 BROKER=kafka:9093


_create_producer_docker:
	( \
	docker exec -it \
	spark-enviroment \
	bash -c "pip install -r /requirements.txt && python3 /python_scripts/producer.py  ${TOPIC} ${DATA_TO_PRODUCE} ${BROKER}";\
	)
# Run it like: make _create_producer_docker	 TOPIC=test1 DATA_TO_PRODUCE=mock_data.json  BROKER=kafka:9093

_create_consumer_docker:
	( \
	docker exec -it \
	spark-enviroment \
	bash -c "pip install -r /requirements.txt && python3 /python_scripts/consumer.py ${TOPIC} ${MODE} ${BROKER}"; \
	)
# make _create_consumer_docker TOPIC=test1 MODE=stdout  BROKER=kafka:9093

_pyspark_docker:
	( \
	docker exec -it \
	spark-enviroment spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5  /python_scripts/job_entry_point.py ${TOPIC} ${BROKER} ${JOB} ${WINDOW_STREAMING_SECS} ${OUTPUT_FORMAT}; \
	)

# Run it like: make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.stream_spark WINDOW_STREAMING_SECS=10 
# Run it like: make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.stream_spark_df WINDOW_STREAMING_SECS=10 OUTPUT_FORMAT=parquet
# Run it like: make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.consumer
