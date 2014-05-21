import os
from graphite.plugins.plugin import DBPlugin
from graphite.plugins.plugin_registry import PluginRegistry
from graphite.node import LeafNode, BranchNode
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log

from ceres import CeresTree, CeresNode

class CeresReader(object):
  __slots__ = ('ceres_node', 'real_metric_path')
  supported = True

  def __init__(self, ceres_node, real_metric_path):
    self.ceres_node = ceres_node
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    intervals = []
    for info in self.ceres_node.slice_info:
      (start, end, step) = info
      intervals.append( Interval(start, end) )

    return IntervalSet(intervals)

  def fetch(self, startTime, endTime):
    data = self.ceres_node.read(startTime, endTime)
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


class CeresFinder:
  def __init__(self, directory):
    self.directory = directory
    self.tree = CeresTree(self.directory)

  def find_nodes(self, query):
    for fs_path in glob( self.tree.getFilesystemPath(query.pattern) ):
      metric_path = self.tree.getNodePath(fs_path)

      if CeresNode.isNodeDir(fs_path):
        ceres_node = self.tree.getNode(metric_path)

        if ceres_node.hasDataForInterval(query.startTime, query.endTime):
          real_metric_path = get_real_metric_path(fs_path, metric_path)
          reader = CeresReader(ceres_node, real_metric_path)
          yield LeafNode(metric_path, reader)

      elif isdir(fs_path):
        yield BranchNode(metric_path)



