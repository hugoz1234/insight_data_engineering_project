from __future__ import print_function
import datetime
import sys
import json

from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils

KAFKA_TOPIC = "yelp_traffic_data"

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def get_day(timestamp):
    """Return the day represetented by timestamp"""
    return timestamp.split(" ")[0]

def get_json_rep(formatted_data):
    """Converts data into json format for later handling"""
    data = {}
    data['business_id'] = formatted_data[0][0]
    data['day'] = formatted_data[0][1]
    data['visit_time'] = formatted_data[0][2]
    data['visits'] = formatted_data[1]

    return json.dumps(data)

def strip_microseconds(dt):
    """Remove microseconds from datetime string rep"""
    dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
    return str(dt.replace(microsecond=0))

def process_to_traffic_table(rdd):
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        df = spark.read.json(rdd)
        df.show()
        df.write.format('org.apache.spark.sql.cassandra') \
        .mode('append') \
        .options(table='traffic_data', keyspace='yelp_data') \
        .save()
    except Exception as e:
        print ('Failed to insert because: ', e)

def process_to_latest_traffic_table(rdd):
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        df = spark.read.json(rdd)
        df.show()
        df.write.format('org.apache.spark.sql.cassandra') \
        .mode('append') \
        .options(table='latest_traffic_data', keyspace='yelp_data') \
        .save()
    except Exception as e:
        print ('Failed to insert because: ', e)

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: streaming <bootstrap.servers>", file=sys.stderr)
    #    exit(-1)

    sc = SparkContext(appName="asdfasdf")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 3)

    kafkaStream = KafkaUtils.createDirectStream(ssc,
                                                [KAFKA_TOPIC],
                                                {"bootstrap.servers": 'localhost:9092'})
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    user_ids_formatted = parsed.map(lambda x: (x['user_id'], 1))
    user_ids_counted = user_ids_formatted.reduceByKey(add)

    formatted = parsed.map(lambda x: ((x['business_id'], get_day(x['visit_time']), strip_microseconds(x['visit_time'])),  1))
    counted = formatted.reduceByKey(add)
    #counted.pprint()
    json_format = counted.map(lambda x: get_json_rep(x))
    
    json_format.foreachRDD(process_to_traffic_table)
    json_format.foreachRDD(process_to_latest_traffic_table)

    ssc.start()
    ssc.awaitTermination()