Kafka*Docker*Solution
========================

## Minimum requirements

* Docker 19.03.5
* Make

# Description

### Infraestructure

This is a high level view of the project.
There will be a cluster of 3 docker containers with Kafka, Zookeeper and a Spark enviroment running.


![Alt text](diagram.png?raw=true "Optional Title")

For this MVP, only a one single broker will be spun up.

### Logic of the workflow

* Spin up cluster 
* Create Kafka Topic
* Create Kafka Producer that will produce the data `data_to_load/mock_data.json` into the broker.
* Create Kafka Consumer
* Stop the cluster.


### How to achive this logic

* Via Make commands from the command line/Terminal

### Let's get started

##### Change to the Repo Directory


* Git clone the repo 
* cd to `challenge_d` directory.

##### Spin up cluster 

* run `make start_cluster` : Then the 3 docker containers will start

##### Create topic
 
* run: `make _create_topic_docker TOPIC=test1 PARITITIONS=1 REPLICATION=1 BROKER=kafka:9093` 
  * This will create a topic named `test1` , with 1 partition, 1 replicationm conneccted to the host and port `kafka:9093`. It is important to use this `kafka:9093` because this is the host and port taht allos you to expose the data oytside the container.

##### Create Producer

* run `make _create_producer_docker	 TOPIC=test1 DATA_TO_PRODUCE=mock_data.json  BROKER=kafka:9093`
  * This will send the data in the file `mock_data.json` under the path /data_to_load/ to the broker. Again the broker should be `kafka:9093`.

##### Create Consumer

* The consumer runs from the Spark Container. It run spark applications against the Kafka broker. It has the following structure: An there is an entry point for the consumer, through `job_entry_point.py`, and executes jobs under the `jobs` folder.

![Alt text](diagram_spark.png?raw=true "Optional Title")

* You can run them like this:
  * `make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.consumer` . Runs a simple consumer that prints data to stdout.
  * `make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.stream_spark WINDOW_STREAMING_SECS=10`. Runs a consumer via Spark Streaming using RDDs and it computes some metrics in the data streamed in the current window that are save to the `output` folder,together with the raw data.
    * Metrics:
      * Country count per streamed window.
      * Unique user count per streamed window.
      * Numnber of male and female user  per streamed window.
      * Using the IP addrees,we get data ,like hostname,city,region ,country,loc,postal and timezone.
  * `make _pyspark_docker TOPIC=test1 BROKER=kafka:9093 JOB=jobs.stream_spark_df WINDOW_STREAMING_SECS=10 OUTPUT_FORMAT=parquet` Runs a consumer via Spark Streaming using Dataframes.In this case the data is consumed with out calculating any metric.

##### Close and Destroy The Cluster

* run: `make stop_cluster`
