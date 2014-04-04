from bisect import bisect_left, bisect_right
import itertools
import logging
import os
import sys

import pycassa
from pycassa import ConnectionPool, ColumnFamily, NotFoundException
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.system_manager import SystemManager, time
from pycassa import types as pycassa_types

DEFAULT_TIMESTEP = 60
DEFAULT_SLICE_CACHING_BEHAVIOR = 'none'

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
    write_consistency_level=ConsistencyLevel.ONE, 
    localDCName=None):

    self.cassandra_connection = ConnectionPool(keyspace, server_list)
    self.cfCache = ColumnFamilyCache(self.cassandra_connection,
      read_consistency_level, write_consistency_level)
    self.root = root
    self._nodeCache = NodeCache()
    self.localDCName = localDCName
    
  def __repr__(self):
    return "<DataTree[0x%x]: %s>" % (id(self), self.root)
  __str__ = __repr__

  def hasNode(self, nodePath):
    """Returns whether the Ceres tree contains the given metric"""
    
    existing = self._nodeCache.get(nodePath)
    if existing is not None:
      return True
    return DataNode.exists(self, nodePath)

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
      searchNodes = list(nodePath)
    
    missingNodes = []
    foundNodes = {}
    for path in searchNodes:
      existing = self._nodeCache.get(path)
      if existing is None:
        missingNodes.append(path)
      else:
        foundNodes[path] = existing

    if not missingNodes:
      if single:
        assert len(foundNodes) == 1
        return foundNodes.values()[0]
      else:
        return foundNodes
      
    for node in DataNode.fromDB(self, missingNodes):
      self._nodeCache.add(node.nodePath, node)
      foundNodes[node.nodePath] = node

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
    assert node is not None, "Attempt to store data to unknown node %s" % (
      nodePath)
    node.write(datapoints)
    return

  def selfAndChildPaths(self, query, dcName=None, startToken=None, 
    endToken=None):
    """Get a list of the self and childs nodes under the `query`.
     
      Think of it in terms of a glob:
        - * at the top of the tree should return all of the root-level nodes
        - carbon.* should return anything that begins with carbon, but *only*
          replacing .* with the actual value:
          ex. carbon.metrics.
              carbon.tests.
      
      If `dcName` is specified only paths created by daemons running in the 
      given Cassandra Data Centre are returned. The daemons are configured 
      with a Data Centre node in db.conf. If True is passed the localDCName 
      passed when the DataTree was created is used. 
       
      Returns a list of the form [ (path, is_metric), ], includes the node 
      identified by query.
    """

    # TODO: this function has grown, maybe the token slices should be 
    # split out. 
    
    if dcName == True:
      if not self.localDCName:
        raise ValueError("dcName set to True, but localDCName not set")
      dcName = self.localDCName
    cfName = "dc_%s_nodes" % (dcName,) if dcName else "global_nodes" 
    
    if bool(startToken) != bool(endToken):
      raise ValueError("None or both of startToken and endToken must be "\
        "specified.")
    elif startToken and endToken and query:
      raise ValueError("query can not be specified with startToken and "\
        "endToken")

    if query == '*':
      query = 'root'
    elif query:
      query = query.replace('.*', '')

    def _genAll():
      """Generator to yield (key, col, value) triples from reading a row 
      in the nodes CF.
      
      The query is the row key.
      """
      # read all rows in the CF
      try:
        cols = self.cfCache.get(cfName).get(query)
      except (NotFoundException):
        pass
      else:
        for col, value in cols.iteritems():
          yield (query, col, value)
    
    def _genRange():
      """Generator to yield (key, col, value) triples from reading a 
      range of rows in the nodes CF."""
      # only read rows in the specified token range. 
      # returns a generator, no NotFoundExceptions
      # generator batches request to pull cf.buffer_size rows per 
      # cassandra call.
      rangeIter = self.cfCache.get(cfName).get_range(start_token=startToken, 
        finish_token=endToken)

      # we may get duplicate entries because a data node will have it's 
      # own row and be a column in it's parents row. 
      # HACK: skip rows that have child data nodes, this is wasteful but the  
      # alternative is to only record data nodes that are metrics in each DC. 
      for key, cols in rangeIter:
        if cols.get("metric") == "true":
          yield (key, "metric", "true")

    colsIter = _genRange() if startToken else _genAll()
    
    childs = []
    for key, col, value in colsIter:
      if col == 'metric' and value == 'true':
        # the query path is a metric
        childs.append((key, True))
      elif value == 'metric':
        # this is a child metric
        childs.append((col, True))
      else:
        # this is a branch node
        childs.append((col, False))
    return childs
    
def _retentionsFromCSV(csv):
    """Parse the command separated ints in `csv` into a list of tuples
    
    Used to deserialise the retention policy.
    """
    
    ints = map(int, csv.split(','))
    return zip(ints[::2], ints[1::2])
  
def _retentionsToCSV(retentions):
    """Parse the list of int tuples [(1,2),] into a comma separated string
    
    used to serialise the retention policy
    """
    
    return ",".join(
      str(i)
      for i in itertools.chain.from_iterable(retentions)
    )
    
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
               '_meta_data', "cfCache", "_deserialise", 
               "_serialise")

  _deserialise = {
    "aggregationMethod" : lambda x : x,
    "retentions" :  _retentionsFromCSV,
    "startTime" : lambda x : int(x),
    "timeStep" : lambda x : int(x),
    "xFilesFactor" : lambda x : float(x),
  }

  _serialise = {
    "aggregationMethod" : lambda x : x,
    "retentions" :  _retentionsToCSV,
    "startTime" : lambda x : str(x),
    "timeStep" : lambda x : str(x),
    "xFilesFactor" : lambda x : str(x),
  }
  
  def __init__(self, tree, meta_data, nodePath):
    self.tree = tree
    self.cfCache = tree.cfCache
    self.nodePath = nodePath
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
    self._meta_data = meta_data
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
    node = cls(tree, meta_data, nodePath)
    node.writeMetadata(meta_data)

    return node

  @classmethod
  def fromDB(cls, tree, nodePaths):
    """Read the nodes for the `nodePaths` list from the DB and return a 
    list of :cls:`DataNode`s.
    """
    
    rows = tree.cfCache.get("metadata").multiget(nodePaths, 
      columns=cls._deserialise.keys())
    
    def _unknownCol(x):
      raise RuntimeError("Cannot deserilaise unknown column %s" % (x))
      
    nodes = []
    for rowKey, rowCols in rows.iteritems():
      
      metadata = {
        colName : cls._deserialise.get(colName, _unknownCol)(colValue)
        for colName, colValue in rowCols.iteritems()
      }
      nodes.append(cls(tree, metadata, rowKey))
    return nodes
    
  @classmethod
  def exists(cls, tree, nodePath):
    """Returns True if the node at `nodepath` exists, False otherwise.
    """
    
    try:
      # Faster to read a named column
      tree.cfCache.get("metadata").get(nodePath, columns=["timeStep"])
      return True
    except (NotFoundException):
       return False
    
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

    if not 'startTime' in metadata:
      # Cut off partial seconds.
      metadata['startTime'] = int(time.time())
    self._meta_data.update(metadata)
    
    def _unknownCol(x):
      raise RuntimeError("Cannot deserilaise unknown collumn %s" % (x))
      
    cols = {
      metaName : self._serialise.get(metaName, _unknownCol)(metaValue)
      for metaName, metaValue in self._meta_data.iteritems()
    }
    
    self.tree.cfCache.get("metadata").insert(self.nodePath, cols)

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
    """Get's a list of the slices available for this metric. 
    
    A slice has a start time and a time step, e.g. start at 9:00AM with 60 
    second precision. 
    
    Returns a list of [ (startTime, timeStep)]
    """
    
    # TODO: can / should this be cached ? 
    # Read the timeSteps we have for this node
    key = self.nodePath
    try:
      nodeSlicesCols = self.cfCache.get("node_slices").get(key, 
        column_count=1000)
    except (NotFoundException) as e:
      return []
    
    # call to Cassandra to get the startTime for each timeStep
    slices = []
    for timeStep, defaultStartTime in nodeSlicesCols.iteritems():
      
      try:
        cols = self.cfCache.getTS("ts{0}".format(timeStep)).get(key, column_count=1)
      except (NotFoundException) as e:
        # no data for this time slice yet, but we have it exists because we 
        # have it in node_slices
        slices.append((defaultStartTime, timeStep))
      else:
        slices.append((cols.keys()[0], timeStep))
    
    slices.sort(reverse=True)
    return slices
    
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
      
    origFromTime = fromTime
    origUntilTime = untilTime
    
    # Normalise here so incase there are not slices
    # will normalise again in the loop
    fromTime = int(origFromTime - (origFromTime % self.timeStep) 
      + self.timeStep)
    untilTime = int(origUntilTime - (origUntilTime % self.timeStep) 
      + self.timeStep)
    
    sliceBoundary = None  # to know when to split up queries across slices
    resultValues = []
    earliestData = None

    for dataSlice in self.slices:
      # Normalize the timestamps to fit proper intervals
      # NOTE: this is different to ceres, it normalises using the nodes
      # timeStamp which results in a mis match when trying to None pad 
      # dataPoints in TimsSeriesData because the timestamp do not match
      fromTime = int(origFromTime - (origFromTime % dataSlice.timeStep) 
        + dataSlice.timeStep)
      untilTime = int(origUntilTime - (origUntilTime % dataSlice.timeStep) 
        + dataSlice.timeStep)

      # if the requested interval starts after the start of this slice
      if fromTime >= dataSlice.startTime:
        try:
          series = dataSlice.read(fromTime, untilTime)
        except NoData:
          # amorton: changed from break to continue
          # so that if the slice has no data we look at the next slice
          continue

        earliestData = series.startTime

        rightMissing = (untilTime - series.endTime) / dataSlice.timeStep
        rightNulls = [None for i in range(rightMissing - len(resultValues))]
        resultValues = series.values + rightNulls + resultValues
        break

      # or if slice contains data for part of the requested interval
      elif untilTime >= dataSlice.startTime:
        # Split the request up if it straddles a slice boundary
        if (sliceBoundary is not None) and untilTime > sliceBoundary:
          requestUntilTime = sliceBoundary
        else:
          requestUntilTime = untilTime

        try:
          series = dataSlice.read(dataSlice.startTime, requestUntilTime)
        except NoData:
          continue

        earliestData = series.startTime

        rightMissing = (requestUntilTime - series.endTime) / dataSlice.timeStep
        rightNulls = [None for i in range(rightMissing)]
        resultValues = series.values + rightNulls + resultValues

      # this is the right-side boundary on the next iteration
      sliceBoundary = dataSlice.startTime

    # The end of the requested interval predates all slices
    if earliestData is None:
      # OK to use the DataNode timeStep here.
      seriesTimestep = self.timeStep
      missing = int(untilTime - fromTime) / seriesTimestep
      resultValues = [None for i in range(missing)]
    else:
      # Left pad nulls if the start of the requested interval 
      # predates all slices
      # use the timestamp from the last DataSlice we looked at.
      seriesTimestep = dataSlice.timeStep
      leftMissing = (earliestData - fromTime) / seriesTimestep
      leftNulls = [None for i in range(leftMissing)]
      resultValues = leftNulls + resultValues

    return TimeSeriesData(fromTime, untilTime, seriesTimestep, resultValues)

  def write(self, datapoints):
    with self.cfCache.batchMutator() as batch:
      return self._write_internal(datapoints, batch)

  def _write_internal(self, datapoints, batch):
    """Write the ``datapoints`` to the db using the ``batch`` mutator.
    """
      
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
            self._write_internal(datapoints, batch)  # recurse to retry
            return
          sequence = []
          break

        # sequence straddles the current slice, write the right side
        elif endingTime >= slice.startTime:
          # index of lowest timestamp that doesn't preceed slice.startTime
          boundaryIndex = bisect_left(timestamps, slice.startTime)
          sequenceWithinSlice = sequence[boundaryIndex:]
          # write the leftovers on the next earlier slice
          sequence = sequence[:boundaryIndex]
          slice.write(sequenceWithinSlice, batch=batch)

        if not sequence:
          break
        
        sliceBoundary = slice.startTime
        
      else: # this else if for the `for slice in self.slices` above
        needsEarlierSlice.append(sequence)

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
    'nodePath', 'ttl', "cfCache")

  def __init__(self, node, startTime, timeStep):
    self.node = node
    self.cfCache = node.tree.cfCache
    self.cassandra_connection = node.cassandra_connection
    self.startTime = startTime
    self.timeStep = timeStep
    self.nodePath = str(node.nodePath)

    # TODO: pull from cache within DataNode.readMetadata
    metadata = self.node.readMetadata()
    # retentions should be [ (timestep, retention )], e.g. (5,24) is a 
    # 5 second timestep with 24 values stored (i.e. 2 minutes)
    retentions = list(metadata['retentions'])
    
    # ensure it is sorted by increasing timestep
    retentions.sort(key=lambda x: x[0])
    # find the first retention pair after me
    # if this is the last timestep in the config ok to use my own retention 
    # as nothing will be rolling up this timestep
    pos = min(bisect_right(retentions, (self.timeStep, sys.maxint)), 
      len(retentions)-1)
    next_timestep, next_retention = retentions[pos]
    # 50% fudge factor because we want the data to be there when the rollup
    # scripts run
    self.ttl = next_timestep * next_retention * 1.5

  def __repr__(self):
    return "<DataSlice[0x%x]: %s>" % (id(self), self.nodePath)
  __str__ = __repr__

  @property
  def isEmpty(self):
    """Returns True id the data slice does not contain any data, False
    otherwise."""

    key = self.nodePath
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
    """Returns the end time for the data slice. 
    
    Returns Java long.MAX_VALUE because our data slices in Cassandra have 
    no end time to them. AFAI in Ceres they have an end time because they 
    it's a file based system and they want to delete files. 
    """
    return 9223372036854775807
    
    
  @classmethod
  def create(cls, node, startTime, timeStep):
    dataSlice = cls(node, startTime, timeStep)
    # Record that there is a data slice for this node 
    # Only records the timeStep there as the start time depends on the 
    # data we have in the tsXX row.
    key = node.nodePath
    cols = {
      dataSlice.timeStep : startTime
    }
    dataSlice.cfCache.get("node_slices").insert(key, cols)
    
    return dataSlice

  def read(self, fromTime, untilTime):
    """Return :cls:`TimeSeriesData` for this DataSlice, between ``fromTime``
    and ``untilTime``
    """
    
    timeOffset = int(fromTime) - self.startTime

    if timeOffset < 0:
      raise InvalidRequest("Requested time range ({0}, {1}) preceeds this "\
        "slice: {2}".format(fromTime, untilTime, self.startTime))

    key = self.nodePath
    tsCF = self.cfCache.getTS("ts{0}".format(self.timeStep))
    #TODO: VERY BAD code here to request call columns
    #get the columns in sensible buckets
    try:
      cols = tsCF.get(key, column_start=fromTime,
                          column_finish=untilTime,
                          column_count=1999999999)
    except (NotFoundException) as e:
      raise NoData()
    
    return TimeSeriesData.fromDB(fromTime, untilTime, self.timeStep, 
      cols.items())
    
  def insert_metric(self, metric, isMetric=False, batch=None):
    """Insert the ``metric`` into the global_nodes CF to say it's a
    metric as opposed to a non leaf node."""
    
    myBatch = False
    if batch is None:
      batch = self.cfCache.batchMutator()
      myBatch = True
      
    split = metric.split('.')
    if len(split) == 1:
      batch.insert(self.cfCache.get("global_nodes"), 'root',
        { metric : '' })
      # Record the DC this node was created in, used to segment rollups
      if self.node.tree.localDCName: 
          batch.insert(self.cfCache.get("dc_%s_nodes" % (
            self.node.tree.localDCName,)), "root", { metric : '' })
      else:
        # TODO: log a WARN that we do not know where this node was created
        pass
        
    else:
      next_metric = '.'.join(split[0:-1])
      metric_type =  'metric' if isMetric else ''
      batch.insert(self.cfCache.get("global_nodes"), next_metric,
        {'.'.join(split) : metric_type })
      # Record the DC this node was created in, used to segment rollups
      if self.node.tree.localDCName:
        batch.insert(self.cfCache.get("dc_%s_nodes" % (
          self.node.tree.localDCName,)), next_metric, 
          {'.'.join(split) : metric_type })
      else:
        # TODO: log a WARN that we do not know where this node was created
        pass
        
      self.insert_metric(next_metric, batch=batch)
    
    if myBatch:
      batch.send()
    return

  def write(self, sequence, batch=None):
    """Write the ``sequence`` of metrics into the the tsXX CF for this
    DataSlice, using the ``batch`` mutator.
    """
    
    myBatch = False
    if batch is None:
      batch = self.cfCache.batchMutator()
      myBatch = True
    
    # Write the data points to the tsXX table
    key = self.nodePath
    cols = {
      int(timestamp) : float(value)
      for timestamp, value in sequence
    }
    tsCF = self.cfCache.getTS("ts{0}".format(self.timeStep))
    # The roll up process relies of reading data from a fine archive that has 
    # overflowed, this data is then selected to fill the corse archive. 
    # e.g. with 5s:2m,1m:5m / retentions 5,24,60,5
    # a rollup started at 01:09:28 will read from the ts5 slice between the 
    # start of the slice and 2014-02-11 01:07:30. Data before 
    # 2014-02-11 01:07:30 is considered overflow data. 
    #
    # It then cycles through the rentions in the course archive (ts60) 
    # selecting data from the overflow points that match. 
    # The window for the coarse archive ends at the time the fine archive 
    # starts and goes back in time (precision * retention) so starts at 
    # 2014-02-11 01:02:00 and ends at 2014-02-11 01:07:00
    #
    # In this case it will look for data points from the overflow data in the  
    # bounds: 
    # 2014-02-11 01:02:00 to 2014-02-11 01:03:00
    # 2014-02-11 01:03:00 to 2014-02-11 01:04:00
    # 2014-02-11 01:04:00 to 2014-02-11 01:05:00
    # 2014-02-11 01:05:00 to 2014-02-11 01:06:00
    # 2014-02-11 01:06:00 to 2014-02-11 01:07:00
    # 
    # The aggregate value created from each of those selections from the 
    # over flow will **overwrite** the value on disk for the ts60 slice. 
    # 
    # So we need to have enough data on disk for the ts5 slice to fill all the 
    # retentions for the ts60 slice. In this case we need 5 minutes of 
    # overflow data in the ts5 slice, this is on top of the 2 minutes of 
    # data the ts5 slice retains.  And so forth, for the last slice 
    # we only need to keep the data on disk for the length of it's rentention. 
    batch.insert(tsCF, key, cols, ttl=self.ttl)
    
    # Make sure this node in the data tree is marked as a metric.
    dataTreeCF = self.cfCache.get("global_nodes")
    batch.insert(dataTreeCF, key, {'metric' : 'true'})
    # Record the DC this node was created in, used to segment rollups
    if self.node.tree.localDCName:
      batch.insert(self.cfCache.get("dc_%s_nodes" % (
        self.node.tree.localDCName,)), key, {'metric' : 'true'})
    else:
      # TODO: log a WARN that we do not know where this node was created
      pass
      
    self.insert_metric(key, True, batch=batch)
    
    if myBatch:
      batch.send()
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
  
  @classmethod
  def fromDB(cls, startTime, endTime, timeStep, dataPoints):
    """Create a new instance using the `dataPoints` list of 
    (col, value) / (timestamp, value) tuples.
    
    Cassandra is a sparse store, we store the time and value of the data 
    point. Ceres (and I guess whisper) are fixed, the CeresSlice will pad out 
    zero entries when data is missing. It will even refuse to write to 
    a particular CeresSlice if too many zero pads would be added. 
    
    Other code in the rollups expects the TimeSeries to not contain any gaps. 
    
    To keep things simple we fill pad missing values with None when 
    constructing a new TimeSeries. 
    """
    
    def _safeIter(baseIter, emptyValue):
      while True:
        try:
          yield baseIter.next()
        except (StopIteration):
          yield emptyValue

    # expected time stamps
    timestampIter = _safeIter(iter(xrange(startTime, endTime, timeStep)), 
      None)
    thisTimestamp = timestampIter.next()
    
    dbIter = _safeIter(iter(dataPoints), (None, None) )
    dbTimestamp, dbValue = dbIter.next()
    
    values = []
    while thisTimestamp is not None:
      
      if thisTimestamp == dbTimestamp:
        # match between expected timestamp and db value
        values.append(dbValue)
        thisTimestamp = timestampIter.next()
        dbTimestamp, dbValue = dbIter.next()
      elif thisTimestamp < dbTimestamp:
        # db timestamp has skipped ahead of the expected timestamp
        # happens when there is a gap in the DB data
        # pad with a None and advance to the next expected timestamp.
        values.append(None)
        thisTimestamp = timestampIter.next()
      else:
        # db timestamp is behind the expected timestamp, or the DB timestamp 
        # is None
        # Happens when we do not have enough db data to fill the
        # period.
        # pad with none and advance to the next expected timestamp 
        values.append(None)
        thisTimestamp = timestampIter.next()
    
    # we should have exhausted the db iterator
    dbTimestamp, dbValue = dbIter.next()
    if not dbTimestamp is None:
      raise RuntimeError("Failed to exhaust the DB values, first overflow "\
        "is %s,%s for startTime %s endTime %s timeStep %s" % (dbTimestamp, 
        dbValue, startTime, endTime, timeStep))
    
    return cls(startTime, endTime, timeStep, values)
    
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


class NoData(Exception):
  pass


class NodeNotFound(Exception):
  pass


class InvalidRequest(Exception):
  pass

# TODO: Thise are not raised by the Data Slice
class SliceGapTooLarge(Exception):
  pass


class SliceDeleted(Exception):
  pass


def setDefaultSliceCachingBehavior(behavior):
  # TODO Should this be global?
  global DEFAULT_SLICE_CACHING_BEHAVIOR

  behavior = behavior.lower()
  if behavior not in ('none', 'all', 'latest'):
    raise ValueError("invalid caching behavior '%s'" % behavior)

  DEFAULT_SLICE_CACHING_BEHAVIOR = behavior


def initializeTableLayout(keyspace, server_list, replicationStrategy, 
  strategyOptions, localDCName):

    sys_manager = SystemManager(server_list[0])

    # Make sure the the keyspace exists
    if keyspace not in sys_manager.list_keyspaces():
      sys_manager.create_keyspace(keyspace, replicationStrategy, 
        strategyOptions)
        
    cf_defs = sys_manager.get_keyspace_column_families(keyspace)
    
    # Create UTF8 CF's
    for tablename in ["global_nodes", "metadata"]:
      if tablename not in cf_defs.keys():
        createUTF8ColumnFamily(sys_manager, keyspace, tablename)
    
    if localDCName:
        
        dcNodes = "dc_%s_nodes" % (localDCName,)
        if dcNodes not in cf_defs.keys():
          createUTF8ColumnFamily(sys_manager, keyspace, dcNodes)
    else:
      # TODO Log we do not have the DC name
      pass
      
    if "node_slices" not in cf_defs.keys():
      sys_manager.create_column_family(
        keyspace,
        "node_slices",
        super=False,
        comparator_type=pycassa_types.LongType(),
        key_validation_class=pycassa_types.UTF8Type(),
        default_validation_class=pycassa_types.LongType()      
      )
  
def createTSColumnFamily(servers, keyspace, tableName):
  """Create a tsXX Column Family using one of the servers in the ``servers``
  list and the ``keysapce`` and ``tableName``.
  """

  for server in servers:
    try:
      SystemManager(server).create_column_family(
          keyspace,
          tableName,
          super=False,
          comparator_type=pycassa_types.LongType(),
          key_validation_class=pycassa_types.UTF8Type(),
          default_validation_class=pycassa_types.FloatType()
      )
      return None
    except (Exception) as e:
      # TODO: log when we know how to log
      lastError = e

  raise RuntimeError("Failed to create CF %s.%s using the server list %s, "\
    "last error was %s" % (keyspace, tableName, servers, str(lastError)))
  
def createUTF8ColumnFamily(sys_manager, keyspace, tablename):
  """Create column family with UTF8Type comparator, value and key."""
  sys_manager.create_column_family(
      keyspace,
      tablename,
      super=False,
      comparator_type=pycassa_types.UTF8Type(),
      key_validation_class=pycassa_types.UTF8Type(),
      default_validation_class=pycassa_types.UTF8Type()
  )
