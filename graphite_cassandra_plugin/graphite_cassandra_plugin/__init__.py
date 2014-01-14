
class GraphiteCassandraPlugin(object):

  def __init__(self, settings):
    self.keyspace = settings.CASSANDRA_KEYSPACE
    self.servers = settings.CASSANDRA_SERVERS
    
    self.finder = graphite_cassandra_plugin.CassandraFinder(self.keyspace, 
      self.servers)

  def finder(self):
    return self.finder
