import json
import random
import sys
import six
import uuid
import operator
import time
# import boto3
from datetime import datetime, timedelta
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer, KafkaProducer

YELP_TRAFFIC_TOPIC = 'yelp_traffic_data'

def generate_random_id():
    return str(uuid.uuid4())

def get_business_ids():
    # TODO remove dependence on having business.json on disk, instead read from s3
    business_ids = set()
    with open('business.json') as data_file:
        for business_json in data_file:
            business = json.loads(business_json)
            business_ids.add(business['business_id'].encode('utf-8'))

    return business_ids

def get_nyc_business_ids():
    # TODO eventually move csv to s3 instead of on disk
    business_ids = set()

    LIMIT = 0

    with open('nyc_businesses.csv') as data_file:
        for line in data_file:
            values = line.split(',')
            business_ids.add(values[0])
            LIMIT += 1
            if LIMIT == 10:
                break
        print ('grabbed ', len(business_ids), ' businesses in nyc')
    return business_ids

class TrafficProducer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        #self.producer = KeyedProducer(self.client)
        self.producer = KafkaProducer(bootstrap_servers=addr, value_serializer=lambda v: json.dumps(v).encode('utf8'))
        self.business_ids = get_nyc_business_ids()

    def generate_record(self):
        random_id = random.sample(self.business_ids, 1)[0]
        formatted_ts = str(datetime.now())
        record = "GET;" + random_id + ';' + formatted_ts
        return record

    def produce_msgs(self, source_symbol):
        """Produces ~200 events per second"""
        # TODO: put correctly formatted traffic data on s3
        # s3 = boto3.resource('s3')
        # bucket = s3.Bucket('insightyelpdata')
        # bucket.download_file('fake_data', 'fake_data.txt')
        while True:
            record = self.generate_record()
            fields = record.split(";")
            message_info = {'request_type':fields[0],
                            'business_id': fields[1],
                            'visit_time':fields[2],
                            'user_id': generate_random_id()}
            # byte_source_symbol = source_symbol.encode('utf8')
            # byte_message_info = json.dumps(message_info).encode('utf8')
            # self.producer.send_messages(YELP_TRAFFIC_TOPIC, byte_source_symbol, byte_message_info)
            self.producer.send(YELP_TRAFFIC_TOPIC, value=message_info)


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = TrafficProducer(ip_addr)
    prod.produce_msgs(partition_key)