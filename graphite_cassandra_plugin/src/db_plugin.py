from plugin_registry import BasePlugin, PluginType

class DBPlugin(BasePlugin):
  def __init__(self, name, finder = None, reader = None):
    super(DBPlugin, self).__init__(name, PluginType().DB)
    self._finder = finder
    self._reader = reader

  # Properties
  @property
  def finder(self):
    return self._finder

  @finder.setter
  def finder(self, finder):
    self._finder = finder

  @property
  def reader(self):
    return self._reader

  @reader.setter
  def reader(self, reader):
    self._reader = reader

