import socket as real_socket
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

import pool

from pycassa.versions import *
import pycassa.api_exceptions

import pycassa.cassandra07.Cassandra
import pycassa.cassandra07.ttypes
import pycassa.cassandra06.Cassandra
import pycassa.cassandra06.ttypes

import pycassa.adapter06

__all__ = ['connect', 'connect_thread_local']

DEFAULT_SERVER = 'localhost:9160'
DEFAULT_PORT = 9160

class Connection(object):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None, credentials=None):
        self.server = server
        server = server.split(':')
        if len(server) == 1:
            port = 9160
        else:
            port = int(server[1])
        host = server[0]
        socket = TSocket.TSocket(host, port)
        if timeout is not None:
            socket.setTimeout(min(500, timeout*1000.0))
        else:
            socket.setTimeout(500)
        if framed_transport:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        # Try 0.7 first
        try06 = False
        try:
            try:
                self.client = pycassa.cassandra07.Cassandra.Client(protocol)
                self.transport.open()

                if credentials is not None:
                    request = pycassa.cassandra07.ttypes.AuthenticationRequest(credentials=credentials)
                    self.client.login(request)
                self.version = int(self.client.describe_version().split('.', 1)[0])
                self.transport.close()
            except (Thrift.TApplicationException, real_socket.error), exc:
                try06 = True
        finally:
            self.transport.close()

        if try06:
            try:
                socket = TSocket.TSocket(host, port)
                if timeout is not None:
                    socket.setTimeout(timeout*1000.0)
                if framed_transport:
                    self.transport = TTransport.TFramedTransport(socket)
                else:
                    self.transport = TTransport.TBufferedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

                self.client = pycassa.cassandra06.Cassandra.Client(protocol)
                self.transport.open()

                if credentials is not None:
                    request = pycassa.cassandra06.ttypes.AuthenticationRequest(credentials=credentials)
                    self.client.login(keyspace, request)
                self.version = int(self.client.describe_version().split('.', 1)[0])
            finally:
                self.transport.close()

        socket = TSocket.TSocket(host, int(port))
        if timeout is not None:
            socket.setTimeout(timeout*1000.0)
        if framed_transport:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        if self.version == CASS_06:
            self.client = pycassa.cassandra06.Cassandra.Client(protocol)
            self.transport.open()
            if credentials is not None:
                request = pycassa.cassandra06.ttypes.AuthenticationRequest(credentials=credentials)
                self.client.login(keyspace, request)
        elif self.version == CASS_07:
            self.client = pycassa.cassandra07.Cassandra.Client(protocol)
            self.transport.open()
            if credentials is not None:
                request = pycassa.cassandra07.ttypes.AuthenticationRequest(credentials=credentials)
                self.client.login(request)
        else:
            assert False

        self.set_keyspace(keyspace)

    def set_keyspace(self, keyspace):
        self.keyspace = keyspace
        if keyspace is not None:
            if self.version == CASS_07:
                self.client.set_keyspace(keyspace)

    def close(self):
        self.transport.close()

    def _call_with_translation(self, f, needs_keyspace, *args, **kwargs):
        try:
            print args, kwargs
            if self.version == CASS_07 or not needs_keyspace:
                return f(*args, **kwargs)
            elif self.version == CASS_06:
                return f(self.keyspace, *args, **kwargs)
        except Exception, exc:
            # Raise an API version independent copy of the exception
            cls = exc.__class__.__name__
            if hasattr(pycassa.api_exceptions, cls):
                why = getattr(exc, 'why', None)
                exc_class = getattr(pycassa.api_exceptions, cls)
                raise exc_class(why)
            else:
                raise

    def cross_version(needs_keyspace):

        def decorator(old_f):

            def new_f(self, *args, **kwargs):
                func = getattr(self.client, old_f.__name__)
                return self._call_with_translation(func, needs_keyspace, *args, **kwargs)

            new_f.__name__ = old_f.__name__
            return new_f

        return decorator

    def only_versions(*versions):

        def decorator(old_f):

            def new_f(self, *args, **kwargs):
                assert self.version in versions, \
                        "The function %s is not available for the version of Cassandra in use" % old_f.__name__
                func = getattr(self.client, old_f.__name__)
                return self._call_with_translation(func, True, *args, **kwargs)

            new_f.__name__ = old_f.__name__
            return new_f

        return decorator

    @cross_version(True)
    def get(self, *args, **kwargs):
        pass

    @cross_version(True)
    def get_slice(self, *args, **kwargs):
        pass

    @cross_version(True)
    def multiget_slice(self, *args, **kwargs):
        pass

    @cross_version(True)
    def get_count(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def multiget_count(self, *args, **kwargs):
        pass

    @cross_version(True)
    def get_range_slices(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def get_indexed_slices(self, *args, **kwargs):
        pass

    @cross_version(True)
    def insert(self, *args, **kwargs):
        pass

    @cross_version(True)
    def batch_mutate(self, *args, **kwargs):
        pass

    @cross_version(True)
    def remove(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def truncate(self, *args, **kwargs):
        pass

    def describe_keyspace(self, keyspace):
        result = self._call_with_translation(self.client.describe_keyspace, False, keyspace)
        if self.version == CASS_06:
            cf_defs = []
            for name, attrs in result.items():
                cf_defs.append(pycassa.adapter06.CfDef(keyspace, name, **attrs))
            return pycassa.adapter06.KsDef(keyspace, cf_defs)
        else:
            return result

    def describe_keyspaces(self):
        result = self._call_with_translation(self.client.describe_keyspaces, False)
        if self.version == CASS_06:
            return [pycassa.adapter06.KsDef(ks, []) for ks in result]
        else:
            return result

    @only_versions(CASS_07)
    def system_add_keyspace(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def system_update_keyspace(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def system_drop_keyspace(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def system_add_column_family(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def system_update_column_family(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def system_drop_column_family(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def describe_schema_versions(self, *args, **kwargs):
        pass

    @cross_version(False)
    def describe_partitioner(self, *args, **kwargs):
        pass

    @only_versions(CASS_07)
    def describe_snitch(self, *args, **kwargs):
        pass

    @cross_version(False)
    def describe_ring(self, *args, **kwargs):
        pass

    @cross_version(False)
    def describe_cluster_name(self, *args, **kwargs):
        pass

    @cross_version(False)
    def describe_version(self, *args, **kwargs):
        pass

def connect(keyspace, servers=None, framed_transport=True, timeout=None,
            credentials=None, retry_time=60, recycle=None, use_threadlocal=True):
    """
    Constructs a :class:`~pycassa.pool.ConnectionPool`. This is primarily available
    for reasons of backwards-compatibility; creating a ConnectionPool directly
    provides more options.  All of the parameters here correspond directly
    with parameters of the same name in
    :meth:`pycassa.pool.ConnectionPool.__init__()`

    """
    if servers is None:
        servers = [DEFAULT_SERVER]
    return pool.ConnectionPool(keyspace=keyspace,
                               server_list=servers,
                               credentials=credentials,
                               timeout=timeout,
                               use_threadlocal=use_threadlocal,
                               prefill=False,
                               pool_size=len(servers),
                               max_overflow=len(servers),
                               max_retries=len(servers),
                               framed_transport=framed_transport)

def connect_thread_local(*args, **kwargs):
    """ Alias of :meth:`connect` """
    return connect(*args, **kwargs)
