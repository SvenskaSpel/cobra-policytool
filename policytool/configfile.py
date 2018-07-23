import json
import os
import platform
from collections import OrderedDict


def _find_default_config():
    if platform.system() != "Windows":
        user_config = os.path.expanduser("~/.config/cobra-policytool/config.json")
        if os.path.exists(user_config):
            return user_config
        if os.path.exists("/etc/cobra-policytool/config.json"):
            return "/etc/cobra-policytool/config.json"
    else:
        user_config = os.path.expanduser("~\cobra-policytool\config.json")
        if os.path.exists(user_config):
            return user_config


class JSONPropertiesFile:

    def __init__(self, config_file_path, default_config={}):
        if config_file_path is None:
            config_file_path = _find_default_config()
        self.default_config = default_config
        self.properties = default_config
        if not config_file_path.endswith(".json"):
            raise ConfigFileError("Must be a JSON file: %s" % config_file_path)
        if os.path.exists(config_file_path):
            with open(config_file_path) as config_file:
                self.properties.update(json.load(config_file, object_pairs_hook=OrderedDict))

    def get(self, environment):
        if environment is None:
            return self.properties
        for env in self.properties['environments']:
            if env['name'] == environment:
                return env
        return self.default_config


class ConfigFileError(Exception):
    pass
