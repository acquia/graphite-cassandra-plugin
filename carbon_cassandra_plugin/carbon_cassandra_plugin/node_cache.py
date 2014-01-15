

class NodeCache(object):
  """A simple caching object to store the datanode objects.
  """
  def __init__(self):
    self.cache = {}

  def add(self, key, value):
    self.cache[key] = value

  def get(self, key):
    try:
      return self.cache[key]
    except AttributeError:
      raise AttributeError("Key %s not found." % key)

  def keys(self):
    return self.cache.keys()
