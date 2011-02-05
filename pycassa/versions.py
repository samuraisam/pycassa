import pycassa.adapter06
import pycassa.adapter07

__all__ = ['CASS_06', 'CASS_07', 'get_adapter_for']

CASS_06 = 2
CASS_07 = 19

def get_adapter_for(api_version):
    if api_version == CASS_06:
        return pycassa.adapter06
    elif api_version == CASS_07:
        return pycassa.adapter07
    else:
        raise Exception("No compatible adapter exists for API version %d." % api_version)
