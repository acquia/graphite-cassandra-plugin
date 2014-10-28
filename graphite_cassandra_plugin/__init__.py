import graphite_cassandra_plugin

class GraphiteCassandraPlugin(object):
  
  def __init__(self, settings):
    self.keyspace = settings.CASSANDRA_KEYSPACE
    self.servers = settings.CASSANDRA_SERVERS
    try:
      self.username = settings.CASSANDRA_USERNAME
      self.password = settings.CASSANDRA_PASSWORD
      credentials = {'username': self.username, 'password': self.password}
    except AttributeError:
      credentials = None
    
    self.finder = graphite_cassandra_plugin.CassandraFinder(self.keyspace, self.servers, credentials)

  def finder(self):
    return self.finder
