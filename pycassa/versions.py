import pycassa.adapter06
import pycassa.adapter07

__all__ = ['CASSANDRA_06_API_VERSION', 'CASSANDRA_07_API_VERSION', 'get_adapter_for']

CASSANDRA_06_API_VERSION = 2
CASSANDRA_07_API_VERSION = 19

CASSANDRA_06 = 6
CASSANDRA_07 = 7

def get_adapter_for(api_version):
    if api_version == CASSANDRA_06_API_VERSION:
        return pycassa.adapter06
    elif api_version == CASSANDRA_07_API_VERSION:
        return pycassa.adapter07
    else:
        raise Exception("No compatible adapter exists for API version %d." % api_version)
