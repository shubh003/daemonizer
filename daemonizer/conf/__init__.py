import os
from importlib import import_module

settings = import_module(os.environ['DAEMONIZER_SETTINGS_MODULE'])