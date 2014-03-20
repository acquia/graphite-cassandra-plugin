import json
import re
import traceback

import carbon_cassandra_db

"""Implements the Carbon API for providing a database. 

This class should implement the interface defined in 
carbon.database.TimeSeriesDatabase. We do not have a import here as this 
class is also used by Graphite and we cannot import carbon from the
running graphite process.

Not that carbon does a bad job of handing errors from here, it normally 
just prints "database write operation failed". So exceptions raised from here
append traceback information to make debugging easier.

"""
class CarbonCassandraDatabase(object):
  plugin_name = 'cassandra'

  def __init__(self, settings):
    self.settings = settings
    self.data_dir = settings['LOCAL_DATA_DIR']
    cassandra_settings = settings['cassandra']
    behavior = cassandra_settings.get('DEFAULT_SLICE_CACHING_BEHAVIOR')
    keyspace = cassandra_settings.get('KEYSPACE')
    servers = cassandra_settings.get('SERVERS')
    servers = [x.strip() for x in servers.split(',')]

    # only regex word chars are allowed in the CF name
    dc_name = re.sub("\W", "_", cassandra_settings.get("LOCAL_DC_NAME")) 

    carbon_cassandra_db.initializeTableLayout(keyspace, servers, 
      cassandra_settings.get("REPLICATION_STRATEGY"), 
      json.loads(cassandra_settings.get("STRATEGY_OPTIONS")), 
      dc_name)

    self.tree = carbon_cassandra_db.DataTree(self.data_dir, keyspace, servers, 
      localDCName=dc_name)

    if behavior:
      carbon_cassandra_db.setDefaultSliceCachingBehavior(behavior)
    if 'MAX_SLICE_GAP' in cassandra_settings:
      carbon_cassandra_db.MAX_SLICE_GAP = int(cassandra_settings['MAX_SLICE_GAP'])

  def write(self, metric, datapoints):
    try:
      self.tree.store(metric, datapoints)
    except (Exception) as e:
      raise RuntimeError("CarbonCassandraDatabase.write error: %s" % (
        traceback.format_exc(e),))

  def exists(self, metric):
    try:
      return self.tree.hasNode(metric)
    except (Exception) as e:
      raise RuntimeError("CarbonCassandraDatabase.exists error: %s" % (
        traceback.format_exc(e),))

  def create(self, metric, **options):

    try:
      # convert argument naming convention
      options['retentions'] = options.pop('retentions')
      options['timeStep'] = options['retentions'][0][0]
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')
      self.tree.createNode(metric, **options)
    except (Exception) as e:
      raise RuntimeError("CarbonCassandraDatabase.create error: %s" % (
        traceback.format_exc(e),))

  def get_metadata(self, metric, key):

    try:
      return self.tree.getNode(metric).readMetadata()[key]
    except (Exception) as e:
      raise RuntimeError("CarbonCassandraDatabase.get_metadata error: %s" % (
        traceback.format_exc(e),))

  def set_metadata(self, metric, key, value):

    try:
      node = self.tree.getNode(metric)
      metadata = node.readMetadata()
      metadata[key] = value
      node.writeMetadata(metadata)
    except (Exception) as e:
      raise RuntimeError("CarbonCassandraDatabase.set_metadata error: %s" % (
        traceback.format_exc(e),))
