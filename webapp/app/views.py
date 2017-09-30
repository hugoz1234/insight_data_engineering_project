from datetime import datetime, timedelta
import json

from flask import jsonify, render_template
from flask_cassandra import CassandraCluster

from app import app
from . import cassandra_grabber


cassandra = CassandraCluster()

app.config['CASSANDRA_NODES'] = ['ec2-34-235-10-75.compute-1.amazonaws.com']


@app.route('/')
@app.route('/index')
def index():
    session = cassandra.connect()
    session.set_keyspace("yelp_data")
    data = cassandra_grabber.get_data(session)
    # print ('********', data[0], data[1])
    return render_template('index.html', data1=json.dumps(data[0]), data2=json.dumps(data[1]))

@app.route('/batch')
def batch():
    return render_template('batch_views.html')


