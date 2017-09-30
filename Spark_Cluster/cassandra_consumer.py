from __future__ import print_function
import datetime
import sys
import json

from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        df = spark.read.json(rdd)
    df.show()
    idk = df.sql("SELECT visit_time FROM yelp_data")
    raise Exception('***************', type(df))
    #print ('********************', type(df))
    #raise Exception('done w/test')

        # Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("traffic_data")

        sqlDF = spark.sql("SELECT business_id, visit_time, user_id FROM traffic_data")

        sqlDF.show()

        sqlDF.write \
             .format("org.apache.spark.sql.cassandra") \
             .mode('append') \
             .options(table="traffic_data", keyspace="yelp_data") \
             .save()
    except Exception as e:
    #TODO: decode using utf8 and avoid error: 'ascii' codec can't encode character u'\xe9' in position 909: ordinal not in range(128)
    print ('Failed to insert becasue: ', e)

def process_v2(rdd):
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

#def process_dstream(dstream):
#    spark = getSparkSessionInstance(

def get_day(timestamp):
    """Return the day represetented by timestamp"""
    return timestamp.split(" ")[0]

def get_time_bucket(timestamp):
    """Return the 30-second interval time bucket timestamp belongs to"""
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    if dt.second < 30:
    return str(dt.replace(second=0, microsecond=0))
    else:
    return str(dt.replace(second=30, microsecond=0))

def get_json_rep(formatted_data):
    """Converts data into json format for later handling"""
    data = {}
    data['business_id'] = formatted_data[0][0]
    data['day'] = formatted_data[0][1]
    data['visit_time'] = formatted_data[0][2]
    data['visits'] = formatted_data[1]
    data['user_id'] = formatted_data[0][3]

    return json.dumps(data)

def strip_microseconds(dt):
    """Remove microseconds from datetime string rep"""
    dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
    return str(dt.replace(microsecond=0))

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: streaming <bootstrap.servers>", file=sys.stderr)
    #    exit(-1)

    sc = SparkContext(appName="asdfasdf")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 3)

    kafkaStream = KafkaUtils.createDirectStream(ssc,
                                                ["yelp_traffic_data_v2"],
                                                {"bootstrap.servers": 'ec2-34-234-23-197.compute-1.amazonaws.com:9092'})
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    formatted = parsed.map(lambda x: ((x['business_id'], get_day(x['visit_time']), strip_microseconds(x['visit_time']), x['user_id']),  1))
    counted = formatted.reduceByKey(add)
    #counted.pprint()
    json_format = counted.map(lambda x: get_json_rep(x))
    json_format.pprint()
    print ('************************************')
    json_format.foreachRDD(process_v2)
    #raise Exception('FFFFFFFFFFFFFFFF')


    #raise Exception('FFFFFFFFFFFFFFFF')
    #parsed.count().map(lambda x:'Vists in this batch: %s' % x).pprint()

    #business_dstream = parsed.map(lambda request: request['business_id'])
    #business_counts = business_dstream.countByValue()

    #business_counts.pprint()
    #print ('************************', type(business_counts))


    #lines = kafkaStream.map(lambda x: x[1])
    #lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()