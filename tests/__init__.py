from pycassa.system_manager import *
from pycassa.versions import *
from nose.plugins.skip import *

TEST_KS = 'PycassaTestKeyspace'

conn = None
try:
    conn = pycassa.connection.Connection(None, 'localhost:9160', True, 0.5, None)
    if conn.version != CASS_07:
        print conn.version
        raise SkipTest('Cassandra 0.7.x not found')
except Exception, exc:
        raise SkipTest()
finally:
    if conn is not None:
        conn.close()

def setup_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    try:
        sys.create_keyspace(TEST_KS, 1)
        sys.create_column_family(TEST_KS, 'Standard1')
        sys.create_column_family(TEST_KS, 'Super1', super=True)
        sys.create_column_family(TEST_KS, 'Indexed1')
        sys.create_index(TEST_KS, 'Indexed1', 'birthdate', LONG_TYPE)
    except:
        if TEST_KS in sys.list_keyspaces():
            sys.drop_keyspace(TEST_KS)
        raise
    sys.close()

def teardown_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    sys.close()
