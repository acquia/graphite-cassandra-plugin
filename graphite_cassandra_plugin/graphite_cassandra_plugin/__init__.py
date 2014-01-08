from graphite.database import GraphiteDatabase

try:
  import graphite_cassandra_plugin
except ImportError:
  pass
else:
  class GraphiteCassandraPlugin(GraphiteDatabase):
    plugin_name = 'cassandra'

    def __init__(self, settings):
      self.keyspace = settings.CASSANDRA_KEYSPACE
      self.servers = settings.CASSANDRA_SERVERS
      
      self.finder = graphite_cassandra_plugin.CassandraFinder(self.keyspace, self.servers)

    def finder(self):
      return self.finder
