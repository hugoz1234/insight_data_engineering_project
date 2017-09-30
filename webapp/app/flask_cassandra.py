# github project source: https://github.com/TerbiumLabs/flask-cassandra/blob/master/flask_cassandra.py#L11

from cassandra.cluster import Cluster
import logging

from flask import current_app

log = logging.getLogger(__name__)

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack


try:
    unicode
except NameError:
    unicode = str


class CassandraCluster(object):

    def __init__(self, app=None):
        self.app = app
        self.cluster = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('CASSANDRA_CLUSTER', ':memory:')
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    def connect(self):
        log.debug("Connecting to CASSANDRA NODES {}".format(current_app.config['CASSANDRA_NODES']))
        if self.cluster is None:
            if isinstance(current_app.config['CASSANDRA_NODES'], (list, tuple)):
                self.cluster = Cluster(current_app.config['CASSANDRA_NODES'])
            elif isinstance(current_app.config['CASSANDRA_NODES'], (str, unicode)):
                self.cluster = Cluster([current_app.config['CASSANDRA_NODES']])
            else:
                raise TypeError("CASSANDRA_NODES must be defined as a list, tuple, string, or unicode object.")

        online_cluster = self.cluster.connect()
        return online_cluster

    def teardown(self, exception):
        ctx = stack.top
        if hasattr(ctx, 'cassandra_cluster'):
            ctx.cassandra_cluster.shutdown()

    @property
    def connection(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'cassandra_cluster'):
                ctx.cassandra_cluster = self.connect()
            return ctx.cassandra_cluster