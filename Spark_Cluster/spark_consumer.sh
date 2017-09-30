#!/bin/bash
PYSCRP=$1
IP_ADDR=$2
spark-submit --master "spark://$IP_ADDR:7077" --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 $PYSCRP
