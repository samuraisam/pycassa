import unittest

from pycassa.system_manager import *
from pycassa.api_exceptions import *

TEST_KS = 'PycassaTestKeyspace'

class SystemManagerTest(unittest.TestCase):

    def test_system_calls(self):
        sys = SystemManager(framed_transport=False)

        sys.describe_ring(TEST_KS)
        sys.describe_cluster_name()
        sys.describe_version()
        sys.list_keyspaces()
