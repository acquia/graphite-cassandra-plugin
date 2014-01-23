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

     :param keyspace: Cassandra keyspace to search over
     :param server_list: List of Cassandra seeds
  """
  def __init__(self, keyspace, server_list):
    self.directory = "/"
    self.tree = DataTree(self.directory, keyspace, server_list)

  def find_nodes(self, query):

    log.info("CassandraFinder.find_nodes(): query is: %s" % query.pattern)
    value = self.tree.getSliceInfo(query.pattern)
    log.info("CassandraFinder.find_nodes(): values are: {0}".format(value))
    query_path = query.pattern.replace('.*', '')
    log.info("CassandraFinder.find_nodes(): query_path changed to {0}".format(query_path))

    leafs = []
    # TODO Where is this getting called?
    for key in value.keys():
      if key == 'metric' and value[key] == 'true':
        # We have a metric.
        log.info("find_nodes(): LeafNode with query_path %s " % leafs)
        leafs.append(query_path)
      elif value[key] == 'metric':
        log.info("find_nodes(): LeafNode with key %s " % leafs)
        leafs.append(key)
      else:
        log.info("find_nodes(): BranchNode with key %s" % (key,))
        yield BranchNode(key)

    if len(leafs) > 0:
      # Don't make a call to Cassandra if there are no leafs.
      log.info("find_nodes(): calling getNode with multiget %s" % (leafs,))
      # Make a single multiget call to Cassandra with all leaf information
      data_nodes = self.tree.getNode(leafs)

      # 'path' here refers to either 'query_path' or 'key'
      for path, node in data_nodes.items():
        reader = CassandraReader(node, path)
        yield LeafNode(path, reader)
