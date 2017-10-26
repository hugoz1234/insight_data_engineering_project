from datetime import datetime, timedelta
import json

from flask import jsonify, render_template
from flask_cassandra import CassandraCluster
from werkzeug.contrib.cache import SimpleCache

from app import app
from . import cassandra_grabber

CASSANDRA_CLUSTER_PUBLIC_IP = None

cassandra = CassandraCluster()
cache = SimpleCache()

app.config['CASSANDRA_NODES'] = [CASSANDRA_CLUSTER_PUBLIC_IP]

def get_data():
    cache.clear()
    session = cassandra.connect()
    session.set_keyspace("yelp_data")
    business_data, time_series, surge_metrics = cassandra_grabber.get_data(session)
    cache.set('business_data', business_data, 60)
    cache.set('traffic_data', time_series, 60)
    cache.set('scalar_average', surge_metrics[0], 60)
    cache.set('time_series_avg', surge_metrics[1], 60)

@app.route('/')
@app.route('/index')
def index():
    get_data()
    return render_template('index.html')

@app.route('/get_realtime_traffic')
def get_realtime_traffic():
    if cache.get('traffic_data') == None or cache.get('time_series_avg') == None:
        get_data()
    return jsonify(traffic_data=cache.get('traffic_data'), 
                   time_series_avg=cache.get('time_series_avg'))

@app.route('/get_realtime_businesses')
def get_reatime_bussinesses():
    if cache.get('business_data') == None:
        get_data()
    return jsonify(business_data=cache.get('business_data'))

@app.route('/batch')
def batch():
    return render_template('batch_views.html')


