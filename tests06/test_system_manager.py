import unittest

from pycassa.system_manager import *
from pycassa.api_exceptions import *

class SystemManagerTest(unittest.TestCase):

    def test_system_calls(self):
        sys = SystemManager()

        sys.describe_ring('TestKeyspace')
        sys.describe_cluster_name()
        sys.describe_version()
        sys.describe_schema_versions()
        sys.list_keyspaces()
