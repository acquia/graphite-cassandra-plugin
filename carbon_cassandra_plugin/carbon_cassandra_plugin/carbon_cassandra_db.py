from bisect import bisect_left
import itertools
import logging
import os

import pycassa
from pycassa import ConnectionPool, ColumnFamily, NotFoundException
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.system_manager import SystemManager, time, SIMPLE_STRATEGY
from pycassa.types import UTF8Type

DEFAULT_TIMESTEP = 60
DEFAULT_SLICE_CACHING_BEHAVIOR = 'none'

# dev code to log using the same logger as graphite web or carbon
log_info = logging.getLogger("info").info


class NodeCache(object):
  """A cache for :class:`DataNode` objects

  TODO: this may have retention policy and size limits.
  """
  def __init__(self):
    self._cache = {}

  def add(self, key, node):
    """Adds or replaces the `node` in the cache.
    """
    self._cache[key] = node

  def get(self, key):
    """Gets the node from the cache with `key` or None if no key exists.
    """
    try:
      return self._cache[key]
    except (KeyError):
      return None


class ColumnFamilyCache(object):
  """A cache for :class:`pycassa.ColumnFamily` objects.

  These are expensive to create, so we want to avoid doing them in a hot
  code path.

  Also contains logic to create a tsXX CF used to store metric data if it
  does not exist.
  """

  def __init__(self, connectionPool, readCL, writeCL):
    self.connectionPool = connectionPool
    self.readCL = readCL
    self.writeCL = writeCL
    self._cache = {}

  def batchMutator(self):
    """Create a :clas:`pycassa.Mutator` to use for batch mutations.

    The Mutator is constructed to use the connection pool and Consistency
    Levels the cache is configured with."""

    return pycassa.batch.Mutator(self.connectionPool,
      write_consistency_level=self.writeCL)

  def get(self, cfName):
    """Get a non tsXX Column Family with ``cfName``.

    If the CF is not found on the server a
    :exc:`pycassa.NotFoundException` is raised.

    use :attr:`getTS` to get tsXX CF's.
    """

    existing = self._cache.get(cfName)
    if existing is not None:
      return existing

    # See if the CF exists.
    ts_cf = None
    # will raise pycassa.NotFoundException if not there
    ts_cf = ColumnFamily(self.connectionPool, cfName,
      read_consistency_level=self.readCL,
      write_consistency_level=self.writeCL)

    assert ts_cf is not None
    self._cache[cfName] = ts_cf
    return ts_cf


  def getTS(self, cfName):
    """Get the tsXX ColumnFamily with ``cfName``, creating the CF on the
    cluster if necessary.
    """

    try:
      return self.get(cfName)
    except (NotFoundException) as e:
      # we will try to create it.
      pass

    createTSColumnFamily(self.connectionPool.server_list,
      self.connectionPool.keyspace, cfName)
    return self.get(cfName)


class DataTree(object):
  """Represents a tree of Ceres metrics contained within a single path on disk
  This is the primary Ceres API.

  :param root: The directory root of the Ceres tree

  See :func:`setDefaultSliceCachingBehavior` to adjust caching behavior
  """

  def __init__(self, root, keyspace, server_list,
    read_consistency_level=ConsistencyLevel.ONE,
    write_consistency_level=ConsistencyLevel.ONE):

    self.cassandra_connection = ConnectionPool(keyspace, server_list)
    self.cfCache = ColumnFamilyCache(self.cassandra_connection,
      read_consistency_level, write_consistency_level)
    self.root = root
    self._nodeCache = NodeCache()

  def __repr__(self):
    return "<DataTree[0x%x]: %s>" % (id(self), self.root)
  __str__ = __repr__

  def hasNode(self, nodePath):
    """Returns whether the Ceres tree contains the given metric"""

    # TODO: check the cache.
    log_info("DataTree.hasNode(): metadata.get(%s)" % (nodePath,))
    try:
      # Faster to read a named column
      self.cfCache.get("metadata").get(nodePath, columns=["timeStep"])
      return True
    except (NotFoundException):
       return False

  def getNode(self, nodePath):
    """Get's one or more :cls:`DataNode` objects.

    If `nodePath` is a string returns a single :cls:`DataNode`. Otherwise
    `nodePath` is treated as an iterable and a dict of nodePath to
    :cls:`DataNode` objects is returned.

    Raises :exc:`NodeNotFound` is a single node was requested as it was not
    found, or is multiple nodes are requested as not all nodes were found.

    :param nodePath: DataNode to get.

    :returns: A single :cls:`DataNode` or dict as above.
    """
    single = False
    if isinstance(nodePath, basestring):
      searchNodes = [nodePath,]
      single = True
    else:
      searchNodes = nodePath

    foundNodes = {}
    for nodePath in searchNodes:
      existing = self._nodeCache.get(nodePath)
      if existing is not None:
        foundNodes[nodePath] = existing
        searchNodes.remove(nodePath)

    if not searchNodes:
      if single:
        assert len(foundNodes) == 1
        return foundNodes.values()[0]
      else:
        return foundNodes

    log_info("DataTree.getNode(): metadata.multiget(%s)" % (searchNodes,))

    client = self.cfCache.get("metadata")
    rows = client.multiget(searchNodes, columns=['aggregationMethod',
                                                 'retentions',
                                                 'startTime',
                                                 'timeStep',
                                                 'xFilesFactor'])

    for rowKey, rowCols in rows.iteritems():
      node = DataNode(self, rowCols, rowKey)
      self._nodeCache.add(node.nodePath, node)
      foundNodes[rowKey] = node

    if len(searchNodes) == 1 and not foundNodes:
      raise NodeNotFound("DataNode %s not found" % (searchNodes[0],))

    if len(searchNodes) != len(foundNodes):
      missingKeys = set(searchNodes).difference(set(foundNodes.keys()))
      raise NodeNotFound("DataNodes %s not found" % (",".join(missingKeys)))

    # If there is only one result, return the single node.
    if single:
      assert len(foundNodes) == 1
      return foundNodes.values()[0]
    else:
      return foundNodes


  def createNode(self, nodePath, **properties):
    """Creates a new metric given a new metric name and optional per-node
      metadata

      :keyword nodePath: The new metric name.
      :keyword \*\*properties: Arbitrary key-value properties to store as
      metric metadata.

      :returns: :class:`DataNode`
    """
    return DataNode.create(self, properties, nodePath)

  def store(self, nodePath, datapoints):
    """Store a list of datapoints associated with a metric

      :keyword nodePath: The metric name to write to
      :keyword datapoints: A list of datapoint tuples: (timestamp, value)
    """
    # TODO Should we throw an exception here?
    node = self.getNode(nodePath)
    if isinstance(node, DataNode):
      node.write(datapoints)
    else:
      log_info("Node %s does not exist." % (nodePath,))
    return

  def getFilesystemPath(self, nodePath):
    """Get the on-disk path of a Ceres node given a metric name
    """
    return os.path.join(self.root, nodePath.replace('.', os.sep))

  def getNodePath(self, fsPath):
    """Get the metric name of a Ceres node given the on-disk path"""
    return fsPath

  def getSliceInfo(self, query):
    """ Return all slice info for a given query
      This needs to get a single level of the tree
      Think of it in terms of a glob:
        - * at the top of the tree should return all of the root-level nodes
        - carbon.* should return anything that begins with carbon, but *only*
          replacing .* with the actual value:
          ex. carbon.metrics.
              carbon.tests.
    """
    if query == '*':
      query = 'root'
    else:
      query = query.replace('.*', '')

    log_info("DataTree.getSliceInfo(): data_tree_nodes.get(%s)" % (query,))
    try:
      return self.cfCache.get("data_tree_nodes").get(query)
    except (NotFoundException):
      # empty dict to say there is are no sub nodes.
      return {}


class DataNode(object):
  """Represents a single Ceres metric.

    :param tree: The tree reference
    :param meta_data: Metric metadata
    :param nodePath:
    :param fsPath:
  """

  __slots__ = ('tree', 'nodePath', 'fsPath',
               'metadataFile', 'timeStep',
               'sliceCache', 'sliceCachingBehavior', 'cassandra_connection',
               '_meta_data', "cfCache")

  def __init__(self, tree, meta_data, nodePath, fsPath=None):
    self.tree = tree
    self.cfCache = tree.cfCache
    self.nodePath = nodePath
    self.fsPath = nodePath
    #self.metadataFile = os.path.join(fsPath, '.ceres-node')
    self.metadataFile = nodePath
    self.timeStep = None
    # sliceCache is sometimes a list of DataSlice objects and sometimes
    # a single DataSlice object.
    # TODO: Consider make it a list at all times.
    self.sliceCache = None
    self.sliceCachingBehavior = DEFAULT_SLICE_CACHING_BEHAVIOR
    self.cassandra_connection = tree.cassandra_connection

    # Convert columns into readable format.
    self._meta_data = self.fromCols(meta_data)
    self.timeStep = self._meta_data.get("timeStep")

  def __repr__(self):
    return "<DataNode[0x%x]: %s>" % (id(self), self.nodePath)
  __str__ = __repr__

  @classmethod
  def create(cls, tree, meta_data, nodePath):
    """Construct a new DataNode with the `node_path` and `meta_data`
    in the `tree` and store it.
    """
    meta_data.setdefault('timeStep', DEFAULT_TIMESTEP)
    node = cls(tree, meta_data, nodePath, nodePath)
    node.writeMetadata(meta_data)
    return node

  @property
  def slice_info(self):
    return [
      (si.startTime, si.endTime, si.timeStep)
      for si in self.slices
    ]

  def readMetadata(self):
    return self._meta_data

  def writeMetadata(self, metadata):
    """Write metadata out to metadata CF in key-value format."""
    log_info("DataNode.writeMetadata(): metadata.insert(%s)" % (
      self.metadataFile,))

    if not 'startTime' in metadata:
      # Cut off partial seconds.
      metadata['startTime'] = int(time.time())

    # Transform list of tuples into list
    rets = list(itertools.chain.from_iterable(metadata['retentions']))
    # Transform list into stringed list, and then CSV
    metadata['retentions'] = ','.join(map(str, rets))

    # Remap all metadata values into strings.
    for key, value in metadata.iteritems():
      metadata[key] = str(value)

    self._meta_data.update(metadata)
    self.tree.cfCache.get("metadata").insert(self.metadataFile, metadata)

  def fromCols(self, cols):
    """Return column values formatted in anticipated dict.
      - timeStep: store as string, cast to int when read
      - retentions: store as csv, split into tuples on reading. e.g. [[60,1440]] store as "60,1440"
      - xFilesFactor: store as string, cast to double when read
      - startTime: store as string, cast to int when read
      - aggregationMethod: store as string
    """

    if isinstance(cols['retentions'], list):
      # Columns are in correct format already.
      # TODO Is fromCols() getting called multiple times?
      return cols

    cols['timeStep'] = int(cols['timeStep'])
    cols['xFilesFactor'] = float(cols['xFilesFactor'])

    # Convert csv into a list of ints
    # "1,2,3,4" => ['1','2','3','4'] => [1,2,3,4]
    retentions = map(int, cols['retentions'].split(','))
    # Skip over each item in the list and pair them up
    # [..] => [(1,2),(3,4)]
    cols['retentions'] = zip(retentions[::2], retentions[1::2])

    return cols

  @property
  def slices(self):

    # What happens when the sliceCache is null
    if self.sliceCache:
      if self.sliceCachingBehavior == 'all':
        for data_slice in self.sliceCache:
          yield data_slice

      elif self.sliceCachingBehavior == 'latest':
        # TODO: This yielding the entire list, think it should return
        # the last item in the list
        yield self.sliceCache
        infos = self.readSlices()
        # TODO: Why does this skip the first item ?
        for info in infos[1:]:
          # TODO: this is passing startTime and timeStep
          # remove the * call and pass proper args
          yield DataSlice(self, *info)

    else:
      if self.sliceCachingBehavior == 'all':
        # TODO: this is passing startTime and timeStep
        # remove the * call and pass proper args
        self.sliceCache = [
          DataSlice(self, *info)
          for info in self.readSlices()
        ]
        for data_slice in self.sliceCache:
          yield data_slice

      elif self.sliceCachingBehavior == 'latest':
        infos = self.readSlices()
        if infos:
          # TODO: this is passing startTime and timeStep
          # remove the * call and pass proper args
          self.sliceCache = DataSlice(self, *infos[0])
          yield self.sliceCache

        for info in infos[1:]:
          # TODO: this is passing startTime and timeStep
          # remove the * call and pass proper args
          yield DataSlice(self, *info)

      elif self.sliceCachingBehavior == 'none':
        for info in self.readSlices():
          # TODO: this is passing startTime and timeStep
          # remove the * call and pass proper args
          yield DataSlice(self, *info)

      else:
        raise ValueError("invalid caching behavior configured '%s'" % (
          self.sliceCachingBehavior,))

  def readSlices(self):
    return [
        (int(self._meta_data["startTime"]),
        int(self._meta_data["timeStep"]))
    ]

  def setSliceCachingBehavior(self, behavior):
    behavior = behavior.lower()
    if behavior not in ('none', 'all', 'latest'):
      raise ValueError("invalid caching behavior '%s'" % behavior)

    self.sliceCachingBehavior = behavior
    self.sliceCache = None
    return

  def clearSliceCache(self):
    self.sliceCache = None
    return

  def hasDataForInterval(self, fromTime, untilTime):
    slices = list(self.slices)
    if not slices:
      return False

    # Why is this getting the last item in the list and first ?
    # Are these guaranteed to be in descending order ?
    # slices returns DataSlice objects.
    earliestData = slices[-1].startTime
    latestData = slices[0].endTime

    return ((fromTime is None) or (fromTime < latestData)) and \
           ((untilTime is None) or (untilTime > earliestData))

  def read(self, fromTime, untilTime):

    if self.timeStep is None:
      self.readMetadata()

    # Normalize the timestamps to fit proper intervals
    fromTime = int(fromTime - (fromTime % self.timeStep) + self.timeStep)
    untilTime = int(untilTime - (untilTime % self.timeStep) + self.timeStep)

    sliceBoundary = None  # to know when to split up queries across slices
    resultValues = []
    earliestData = None

    # TODO: slice is a global function, rename this var
    # We are iterating over DataSlice objects here
    for slice in self.slices:
      # if the requested interval starts after the start of this slice
      if fromTime >= slice.startTime:
        try:
          series = slice.read(fromTime, untilTime)
        except NoData:
          # Should this break processing, means we only look at one dataslice?
          # it's a continue below
          break

        earliestData = series.startTime

        rightMissing = (untilTime - series.endTime) / self.timeStep
        rightNulls = [None for i in range(rightMissing - len(resultValues))]
        resultValues = series.values + rightNulls + resultValues
        break

      # or if slice contains data for part of the requested interval
      elif untilTime >= slice.startTime:
        # Split the request up if it straddles a slice boundary
        if (sliceBoundary is not None) and untilTime > sliceBoundary:
          requestUntilTime = sliceBoundary
        else:
          requestUntilTime = untilTime

        try:
          series = slice.read(slice.startTime, requestUntilTime)
        except NoData:
          continue

        earliestData = series.startTime

        rightMissing = (requestUntilTime - series.endTime) / self.timeStep
        rightNulls = [None for i in range(rightMissing)]
        resultValues = series.values + rightNulls + resultValues

      # this is the right-side boundary on the next iteration
      sliceBoundary = slice.startTime

    # The end of the requested interval predates all slices
    if earliestData is None:
      missing = int(untilTime - fromTime) / self.timeStep
      resultValues = [None for i in range(missing)]

    # Left pad nulls if the start of the requested interval predates all slices
    else:
      leftMissing = (earliestData - fromTime) / self.timeStep
      leftNulls = [None for i in range(leftMissing)]
      resultValues = leftNulls + resultValues

    return TimeSeriesData(fromTime, untilTime, self.timeStep, resultValues)

  def write(self, datapoints):
    with self.cfCache.batchMutator() as batch:
      return self._write_internal(datapoints, batch=batch)

  def _write_internal(self, datapoints, batch=None):
    """Write the ``datapoints`` to the db using the ``batch`` mutator.
    """

    if batch is None:
      batch = self.cfCache.batchMutator()

    if self.timeStep is None:
      self.readMetadata()

    if not datapoints:
      return

    sequences = self.compact(datapoints)
    needsEarlierSlice = []  # keep track of sequences that precede all existing slices

    while sequences:
      sequence = sequences.pop()
      timestamps = [t for t,v in sequence]
      beginningTime = timestamps[0]
      endingTime = timestamps[-1]
      sliceBoundary = None  # used to prevent writing sequences across slice boundaries
      slicesExist = False

      for slice in self.slices:
        if slice.timeStep != self.timeStep:
          continue

        slicesExist = True

        # truncate sequence so it doesn't cross the slice boundaries
        if beginningTime >= slice.startTime:
          if sliceBoundary is None:
            sequenceWithinSlice = sequence
          else:
            # index of highest timestamp that doesn't exceed sliceBoundary
            boundaryIndex = bisect_left(timestamps, sliceBoundary)
            sequenceWithinSlice = sequence[:boundaryIndex]

          try:
            slice.write(sequenceWithinSlice, batch=batch)
          except SliceGapTooLarge:
            newSlice = DataSlice.create(self, beginningTime, slice.timeStep)
            newSlice.write(sequenceWithinSlice, batch=batch)
            self.sliceCache = None
          except SliceDeleted:
            self.sliceCache = None
            self._write_internal(datapoints, batch=batch)  # recurse to retry
            return

          break

        # sequence straddles the current slice, write the right side
        elif endingTime >= slice.startTime:
          # index of lowest timestamp that doesn't preceed slice.startTime
          boundaryIndex = bisect_left(timestamps, slice.startTime)
          sequenceWithinSlice = sequence[boundaryIndex:]
          leftover = sequence[:boundaryIndex]
          sequences.append(leftover)
          slice.write(sequenceWithinSlice, batch=batch)

        else:
          needsEarlierSlice.append(sequence)

        sliceBoundary = slice.startTime

      if not slicesExist:
        sequences.append(sequence)
        needsEarlierSlice = sequences
        break

    for sequence in needsEarlierSlice:
      # TODO: what is sequence[0][0] returning
      slice = DataSlice.create(self, int(sequence[0][0]), self.timeStep)
      slice.write(sequence, batch=batch)
      self.sliceCache = None
    return


  def compact(self, datapoints):
    datapoints = sorted((int(timestamp), float(value))
                         for timestamp, value in datapoints
                         if value is not None)
    sequences = []
    sequence = []
    minimumTimestamp = 0  # used to avoid duplicate intervals

    for timestamp, value in datapoints:
      timestamp -= timestamp % self.timeStep  # round it down to a proper interval

      if not sequence:
        sequence.append((timestamp, value))

      else:
        if not timestamp > minimumTimestamp:  # drop duplicate intervals
          continue

        if timestamp == sequence[-1][0] + self.timeStep:  # append contiguous datapoints
          sequence.append((timestamp, value))

        else:  # start a new sequence if not contiguous
          sequences.append(sequence)
          sequence = [(timestamp, value)]

      minimumTimestamp = timestamp

    if sequence:
      sequences.append(sequence)

    return sequences


class DataSlice(object):
  __slots__ = ('node', 'cassandra_connection', 'startTime', 'timeStep',
    'fsPath', 'retention', "cfCache")

  def __init__(self, node, startTime, timeStep):
    self.node = node
    self.cfCache = node.tree.cfCache
    self.cassandra_connection = node.cassandra_connection
    self.startTime = startTime
    self.timeStep = timeStep
    self.fsPath = "{0}".format(node.fsPath)

    # TODO: pull from cache within DataNode.readMetadata
    metadata = self.node.readMetadata()
    retention = filter(lambda x:x[0] == self.timeStep, metadata['retentions'])[0]
    self.retention = int(retention[0]) * int(retention[1])

  def __repr__(self):
    return "<DataSlice[0x%x]: %s>" % (id(self), self.fsPath)
  __str__ = __repr__

  @property
  def isEmpty(self):
    """Returns True id the data slice does not contain any data, False
    otherwise."""

    key = str(self.node.fsPath)
    tsCF = self.cfCache.getTS("ts{0}".format(self.timeStep))

    try:
      # will raise NotFoundException if there is no data, test result to be
      # super sure
      cols = tsCF.get(key, column_count=1)
    except (NotFoundException):
      return True
    return True if not (cols) else False

  @property
  def endTime(self):

    key = str(self.node.fsPath)
    tsCF = self.cfCache.getTS("ts{0}".format(self.timeStep))

    try:
      #TODO: do not use reversed, it has bad performance.
      cols = tsCF.get(key, column_reversed=True, column_count=1)
    except (NotFoundException):
      return time.time()

    assert len(cols) == 1
    return int(cols.keys()[-0])

  @classmethod
  def create(cls, node, startTime, timeStep):
    slice = cls(node, startTime, timeStep)
    return slice

  def read(self, fromTime, untilTime):
    """Return :cls:`TimeSeriesData` for this DataSlice, between ``fromTime``
    and ``untilTime``
    """

    timeOffset = int(fromTime) - self.startTime

    if timeOffset < 0:
      raise InvalidRequest("Requested time range ({0}, {1}) preceeds this "\
        "slice: {2}".format(fromTime, untilTime, self.startTime))

    key = str(self.node.fsPath)
    tsCF = self.cfCache.getTS("ts{0}".format(self.timeStep))
    #TODO: VERY BAD code here to request call columns
    #get the columns in sensible buckets

    cols = tsCF.get(key, column_start="{0}".format(fromTime),
                        column_finish="{0}".format(untilTime),
                        column_count=1999999999)
    if not cols:
      raise NoData()

    endTime = cols.keys()[-1]
    values = [float(x) for x in cols.values()]
    return TimeSeriesData(fromTime, int(endTime), self.timeStep, values)

  def insert_metric(self, metric, isMetric=False, batch=None):
    """Insert the ``metric`` into the data_tree_nodes CF to say it's a
    metric as opposed to a non leaf node."""

    if batch is None:
      batch = self.cfCache.batchMutator()

    split = metric.split('.')
    if len(split) == 1:
      batch.insert(self.cfCache.get("data_tree_nodes"), 'root',
        { metric : '' })
    else:
      next_metric = '.'.join(split[0:-1])
      metric_type =  'metric' if isMetric else ''
      batch.insert(self.cfCache.get("data_tree_nodes"), next_metric,
        {'.'.join(split) : metric_type })
      self.insert_metric(next_metric, batch=batch)
    return

  def write(self, sequence, batch=None):
    """Write the ``sequence`` of metrics into the the tsXX CF for this
    DataSlice, using the ``batch`` mutator.
    """

    if batch is None:
      batch = self.cfCache.batchMutator()

    key = str(self.node.fsPath)
    cols = dict(
      (str(t), str(v))
      for t, v in sequence
    )
    tableName = "ts{0}".format(self.timeStep)

    tsCF = self.cfCache.getTS(tableName)

    log_info("DataSlice.write() 1: %s.insert(%s)" % (tableName, key,))
    batch.insert(tsCF, key, cols, ttl=self.retention)

    # update the slide info for the timestamp lookup
    if not self.node.readMetadata().get("startTime"):
      meta = self.node.readMetadata()
      meta["startTime"] = self.startTime
      # TODO: use the batch
      self.node.writeMetadata(meta)

    dataTreeCF = self.cfCache.get("data_tree_nodes")
    log_info("DataSlice.write() 4: data_tree_nodes.insert(%s)" % (key,))
    batch.insert(dataTreeCF, key, {'metric' : 'true'})
    self.insert_metric(key, True, batch=batch)

    return

  def __cmp__(self, other):
    return cmp(self.startTime, other.startTime)



class TimeSeriesData(object):
  __slots__ = ('startTime', 'endTime', 'timeStep', 'values')

  def __init__(self, startTime, endTime, timeStep, values):
    self.startTime = startTime
    self.endTime = endTime
    self.timeStep = timeStep
    self.values = values

  @property
  def timestamps(self):
    return xrange(self.startTime, self.endTime, self.timeStep)

  def __iter__(self):
    return itertools.izip(self.timestamps, self.values)

  def __len__(self):
    return len(self.values)

  def merge(self, other):
    for timestamp, value in other:
      if value is None:
        continue

      timestamp -= timestamp % self.timeStep
      if timestamp < self.startTime:
        continue

      index = int((timestamp - self.startTime) / self.timeStep)

      try:
        if self.values[index] is None:
          self.values[index] = value
      except IndexError:
        continue


class CorruptNode(Exception):
  def __init__(self, node, problem):
    Exception.__init__(self, problem)
    self.node = node
    self.problem = problem


class NoData(Exception):
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


class NodeNotFound(Exception):
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


class NodeDeleted(Exception):
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


class InvalidRequest(Exception):
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


class SliceGapTooLarge(Exception):
  "For internal use only"
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


class SliceDeleted(Exception):
  def __init__(self, problem):
    Exception.__init__(self, problem)
    self.problem = problem


def setDefaultSliceCachingBehavior(behavior):
  # TODO Should this be global?
  global DEFAULT_SLICE_CACHING_BEHAVIOR

  behavior = behavior.lower()
  if behavior not in ('none', 'all', 'latest'):
    raise ValueError("invalid caching behavior '%s'" % behavior)

  DEFAULT_SLICE_CACHING_BEHAVIOR = behavior


def initializeTableLayout(keyspace, server_list=[]):
    try:
      # TODO move functionality out of try statement
      cass_server = server_list[0]
      sys_manager = SystemManager(cass_server)

      # Make sure the the keyspace exists
      if keyspace not in sys_manager.list_keyspaces():
        sys_manager.create_keyspace(keyspace, SIMPLE_STRATEGY, \
                {'replication_factor': '3'})

      cf_defs = sys_manager.get_keyspace_column_families(keyspace)

      # Loop through and make sure that the necessary column families exist
      for tablename in ["data_tree_nodes", "metadata"]:
        if tablename not in cf_defs.keys():
          createColumnFamily(sys_manager, keyspace, tablename)
    except Exception as e:
      raise Exception("Error initalizing table layout: {0}".format(e))


def createTSColumnFamily(servers, keyspace, tableName):
  """Create a tsXX Column Family using one of the servers in the ``servers``
  list and the ``keysapce`` and ``tableName``.
  """

  for server in servers:
    try:
      sysManager = SystemManager(server)
      createColumnFamily(sysManager, keyspace, tableName)
    except (Exception) as e:
      # TODO: log when we know how to log
      continue
    return None

  raise RuntimeError("Failed to create CF %s.%s using the server list %s" % (
    keyspace, tableName, servers))

def createColumnFamily(sys_manager, keyspace, tablename):
  """Create column family with UTF8Type comparators."""
  sys_manager.create_column_family(
      keyspace,
      tablename,
      super=False,
      comparator_type=UTF8Type(),
      key_validation_class=UTF8Type(),
      default_validation_class=UTF8Type()
  )
