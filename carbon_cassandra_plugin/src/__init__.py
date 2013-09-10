
from carbon.util import PluginRegistrar
from carbon.database import TimeSeriesDatabase

try:
  import carbon_cassandra_db
except ImportError:
  pass
else:
  class CarbonCassandraDatabase(TimeSeriesDatabase):
    plugin_name = 'cassandra'

    def __init__(self, settings):
      self.settings = settings
      self.data_dir = settings['LOCAL_DATA_DIR']
      cassandra_settings = settings['cassandra']
      behavior = cassandra_settings.get('DEFAULT_SLICE_CACHING_BEHAVIOR')
      keyspace = cassandra_settings.get('KEYSPACE')
      servers = cassandra_settings.get('SERVERS')
      servers = [x.strip() for x in servers.split(',')]

      carbon_cassandra_db.initializeTableLayout(keyspace, servers)

      self.tree = carbon_cassandra_db.DataTree(self.data_dir, keyspace, servers)

      if behavior:
        carbon_cassandra_db.setDefaultSliceCachingBehavior(behavior)
      if 'MAX_SLICE_GAP' in cassandra_settings:
        carbon_cassandra_db.MAX_SLICE_GAP = int(cassandra_settings['MAX_SLICE_GAP'])

    def write(self, metric, datapoints):
      try:
        self.tree.store(metric, datapoints)
      except Exception as e:
        raise Exception("CarbonCassandraDatabase.write error: %s" % str(e))

    def exists(self, metric):
      return self.tree.hasNode(metric)

    def create(self, metric, **options):
      # convert argument naming convention
      options['retentions'] = options.pop('retentions')
      options['timeStep'] = options['retentions'][0][0]
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')
      self.tree.createNode(metric, **options)

    def get_metadata(self, metric, key):
      return self.tree.getNode(metric).readMetadata()[key]

    def set_metadata(self, metric, key, value):
      node = self.tree.getNode(metric)
      metadata = node.readMetadata()
      metadata[key] = value
      node.writeMetadata(metadata)

