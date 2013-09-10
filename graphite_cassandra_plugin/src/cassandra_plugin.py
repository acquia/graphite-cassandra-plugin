import os
from graphite.plugins.plugin import DBPlugin
from graphite.plugins.plugin_registry import PluginRegistry
from graphite.node import LeafNode, BranchNode
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log

from graphite_cassandra_plugin import DataTree

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

class CassandraFinder:
  def __init__(self, directory, keyspace, servers):
    self.directory = directory
    self.tree = DataTree(directory, keyspace, servers)
    #log.exception(self.tree.getSliceInfo(''))

  def find_nodes(self, query):
    values = self.tree.getSliceInfo(query.pattern)
    log.exception(values)

    for fs_path in values:
      cassandra_node = self.tree.getNode(fs_path)

      if cassandra_node.hasDataForInterval(query.startTime, query.endTime):
        reader = CassandraReader(cassandra_node, fs_path)
        yield LeafNode(fs_path, reader)
      #elif cassandra_node.isNodeDir(fs_path):
      elif self.tree.hasNode(fs_path):
        yield BranchNode(fs_path)

class CassandraPlugin(DBPlugin):

  def __init__(self, keyspace, servers):
    super(CassandraPlugin, self).__init__('CassandraPlugin')
    self.finder(CassandraFinder(keyspace, servers))
    self.reader(CassandraReader())
    PluginRegistry().add_plugin(self)

# This is old. Should be replaced with the data_tree_node query in CarbonLink
#def cassandra_json():
#  matches = []
#  try:
#    metrics = tree.getSliceInfo('')
#    if metrics:
#      for filename, _ in metrics:
#        sep_path = os.path.join(filename.replace('.', os.sep))
#        matches.append(sep_path)
#  except:
#    pass
#
#  return matches

