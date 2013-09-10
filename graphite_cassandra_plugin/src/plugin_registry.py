class PluginType(set):
  def __init__(self):
    self.add('DB')

  def __getattr__(self, name):
    if name in self:
      return name
    raise AttributeError

class BasePlugin(object):
  def __init__(self, name, p_type):
    self._name = name
    self._plugin_type = p_type

  @property
  def plugin_type(self):
    return self._plugin_type

class PluginRegistry(object):
  __registry = {}
  __plugin_types = PluginType()

  def __init__(self):
    self.__dict__ = self.__registry

  def registry(self):
    return self.__dict__

  def add_plugin(self, plugin):
    self.__registry[plugin.name] = plugin

  def get_plugin_for_type(self, plugin_type):
    [p for p in self.__registry if p.plugin_type == plugin_type]
