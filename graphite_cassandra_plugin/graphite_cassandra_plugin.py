from graphite.node import LeafNode, BranchNode
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log

from carbon_cassandra_plugin.carbon_cassandra_db import DataTree

class CassandraReader(object):
  __slots__ = ('cassandra_node', 'real_metric_path')
  supported = True

  def __init__(self, cassandra_node, real_metric_path):
    self.cassandra_node = cassandra_node
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    intervals = []
    for info in self.cassandra_node.slice_info:
      (start, end, step) = info
      intervals.append(Interval(start, end))

    return IntervalSet(intervals)

  def fetch(self, startTime, endTime):
    data = self.cassandra_node.read(startTime, endTime)
    time_info = (data.startTime, data.endTime, data.timeStep)
    values = list(data.values)
    #log.exception("{0}, {1}, {2}".format(data.startTime, data.endTime, data.timeStep))
    #log.exception(values)

    # Merge in data from carbon's cache
    try:
      cached_datapoints = CarbonLink.query(self.real_metric_path)
    except:
      log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
      cached_datapoints = []

    for (timestamp, value) in cached_datapoints:
      interval = timestamp - (timestamp % data.timeStep)

      try:
        i = int(interval - data.startTime) / data.timeStep
        values[i] = value
      except:
        pass

    return (time_info, values)

class CassandraFinder(object):
  """Creates a tree based on the values in Cassandra, and searches
     over it.

     :param config: Configuration provided by the storage api
            if not required)
  """
  def __init__(self, config=None):
    from django.conf import settings
    keyspace = getattr(settings, 'CASSANDRA_KEYSPACE')
    server_list = getattr(settings, 'CASSANDRA_SERVERS')
    credentials = {'username': getattr(settings, 'CASSANDRA_USERNAME'), 'password': getattr(settings, 'CASSANDRA_PASSWORD')}
    self.directory = "/"
    self.tree = DataTree(self.directory, keyspace, server_list, credentials=credentials)

  def find_nodes(self, query):

    childs = self.tree.selfAndChildPaths(query.pattern)
    childNodes = self.tree.getNode([child
      for child, isMetric in childs
      if isMetric
    ])
    
    # make sure we yield in the DB order
    for child, isMetric in childs:
      if isMetric:
        yield LeafNode(child, CassandraReader(childNodes[child], child))
      else:
        yield BranchNode(child)


