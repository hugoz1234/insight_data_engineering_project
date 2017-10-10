#!/bin/bash
PY_SCRP=$1
SPARK_MASTER=$2
CASSANDRA_WORKERS=$3
spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 $PY_SCRP