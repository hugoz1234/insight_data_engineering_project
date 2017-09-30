import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

topic = 'yelp_traffic_data_v2'
brokers_dns_str = '34.234.23.197:9092'
#spark context
sc = SparkContext(appName='PythonSparkStreamingKafka_RM_01')
sc.setLogLevel('WARN')

#streaming context
ssc = StreamingContext(sc, 2)

#kafka connect
kafkaStream = KafkaUtils.createStream(ssc,
                                      'localhost:2181',
                                      'spark-streaming',
                                      {topic:1})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Vists in this batch: %s' % x).pprint()

business_dstream = parsed.map(lambda request: request['url'])
business_counts = business_dstream.countByValue()
business_counts.pprint()

#create SparkSession in case it doesn't exist
def getSparkSessionInstance(sparkConf):
  if ('sparkSessionSingletonInstance' not in globals()):
    globals()['sparkSessionSingletonInstance'] = SparkSession.builder\
      .config(conf=sparkConf)\
      .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def process(rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    collectionsDF = spark.read.json(rdd)
    collectionsDF.write\
        .format('org.apache.spark.sql.cassandra')\
        .mode('append')\
        .options(table='traffic_data', keyspace='yelp_data')\
        .save()

#print ('*****************************')
#cassandra_formatted_rdd = parsed.map(lambda request: {request['url']: {request['timestamp']: request['user_id']}})
#print (type(cassandra_formatted_rdd), cassandra_formatted_rdd)
#cassandra_formatted_rdd.pprint(10)
#cassandra_formatted_rdd.foreachRDD(process)

#parse kafka streams into python dicts
# kvs = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers_dns_str})
# cols = kvs.map(lambda x: x[1]) #map each element in dict to kvs
# cols.foreachRDD(process)

ssc.start()
ssc.awaitTermination(timeout=60)

#ssc.stop()

