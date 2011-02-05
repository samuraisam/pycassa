from pycassa.system_manager import *
from nose.plugins.skip import *

import pycassa.connection
from pycassa.versions import *

conn = None
try:
    conn = pycassa.connection.Connection(None, 'localhost:9160', False, 0.5, None)
    if conn.version != CASS_06:
        raise SkipTest('Cassandra 0.6.x not found')
except Exception, exc:
        raise SkipTest()
finally:
    if conn is not None:
        conn.close()
