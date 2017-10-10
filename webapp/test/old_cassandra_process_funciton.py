# def process(rdd):
#     try:
#         # Get the singleton instance of SparkSession
#         spark = getSparkSessionInstance(rdd.context.getConf())

#         df = spark.read.json(rdd)
#     df.show()
#     idk = df.sql("SELECT visit_time FROM yelp_data")
#     raise Exception('***************', type(df))
#     #print ('********************', type(df))
#     #raise Exception('done w/test')

#         # Creates a temporary view using the DataFrame
#         df.createOrReplaceTempView("traffic_data")

#         sqlDF = spark.sql("SELECT business_id, visit_time, user_id FROM traffic_data")

#         sqlDF.show()

#         sqlDF.write \
#              .format("org.apache.spark.sql.cassandra") \
#              .mode('append') \
#              .options(table="traffic_data", keyspace="yelp_data") \
#              .save()
#     except Exception as e:
#     #TODO: decode using utf8 and avoid error: 'ascii' codec can't encode character u'\xe9' in position 909: ordinal not in range(128)
#     print ('Failed to insert becasue: ', e)