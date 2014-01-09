from graphite.database import GraphiteDatabase

try:
  import graphite_ceres_plugin
except ImportError:
  pass
else:
  class GraphiteCeresPlugin(GraphiteDatabase):
    plugin_name = 'ceres'

    def __init__(self, settings):
      self.directory = settings.CERES_DIR
      self.finder = graphite_ceres_plugin.CeresFinder(self.directory)

    def finder(self):
      return self.finder
