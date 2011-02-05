import pycassa.cassandra06.ttypes as parent
from pycassa.api_exceptions import NotFoundException
import threading

_TYPES = ['BytesType', 'LongType', 'IntegerType', 'UTF8Type', 'AsciiType',
         'LexicalUUIDType', 'TimeUUIDType']

class CfAdapter(object):

    def __init__(self, column_family):
        self._tlocal = threading.local()
        self.cf = column_family
        self.pool = column_family.pool

    def _obtain_connection(self):
        self._tlocal.client = self.pool.get()

    def _release_connection(self):
        if hasattr(self._tlocal, 'client'):
            if self._tlocal.client:
                self._tlocal.client.return_to_pool()
                self._tlocal.client = None

    def load_type_map(self):
        col_fam = None
        try:
            try:
                self._obtain_connection()
                col_fam = self._tlocal.client.get_keyspace_description(
                    use_dict_for_col_metadata=True)[self.cf.column_family]
            except KeyError:
                raise NotFoundException(why='Column family %s not found.' % self.cf.column_family)
        finally:
            self._release_connection()

        if col_fam is not None:
            self.cf.super = col_fam.column_type == 'Super'
            if self.cf.autopack_names:
                if not self.cf.super:
                    self.cf.col_name_data_type = col_fam.comparator_type
                else:
                    self.cf.col_name_data_type = col_fam.subcomparator_type
                    self.cf.supercol_name_data_type = self._extract_type_name(col_fam.comparator_type)

                index = self.cf.col_name_data_type = self._extract_type_name(self.cf.col_name_data_type)
            if self.cf.autopack_values:
                self.cf.cf_data_type = self._extract_type_name(col_fam.default_validation_class)
                self.cf.col_type_dict = dict()
                for name, cdef in col_fam.column_metadata.items():
                    self.cf.col_type_dict[name] = self._extract_type_name(cdef.validation_class)

    def _extract_type_name(self, string):
        if string is None:
            return 'BytesType'
        index = string.rfind('.')
        if index == -1:
            string = 'BytesType'
        else:
            string = string[index + 1: ]
            if string not in _TYPES:
                string = 'BytesType'
        return string

    def get_count(self, key, cp, sp, cl):
        assert sp.column_names is None and (sp.slice_range is None or \
                (sp.slice_range.start == "" and sp.slice_range.finish == "")), \
                "The columns, column_start, and column_finish parameter are " \
                "only available for get_count() in Cassandra 0.7 and later."

        try:
            self._obtain_connection()
            return self._tlocal.client.get_count(key, cp, cl)
        finally:
            self._release_connection()

    def insert(self, key, col, value, timestamp, wcl, supercol=None, ttl=None):
        cp = ColumnPath(self.cf.column_family, supercol, col)
        try:
            self._obtain_connection()
            return self._tlocal.client.insert(key, cp, value, timestamp, wcl)
        finally:
            self._release_connection()

class Column(parent.Column):

    def __init__(self, name, value, timestamp, ttl=None):
        parent.Column.__init__(self, name, value, timestamp)

class SuperColumn(parent.SuperColumn):
    pass

class ColumnOrSuperColumn(parent.ColumnOrSuperColumn):
    pass

class ColumnParent(parent.ColumnParent):
    pass

class ColumnPath(parent.ColumnPath):
    pass

class ConsistencyLevel(parent.ConsistencyLevel):
    pass

class Deletion(parent.Deletion):
    pass

class Mutation(parent.Mutation):
    pass

class SlicePredicate(parent.SlicePredicate):
    pass

class SliceRange(parent.SliceRange):
    pass

class KeyRange(parent.KeyRange):
    pass

class KeySlice(parent.KeySlice):
    pass

class CfDef(object):

    def __init__(self, keyspace, name, **kwargs):
        self.keyspace = keyspace
        self.name = name
        self.column_type = kwargs['Type']
        self.comparator_type = kwargs['CompareWith']
        self.subcomparator_type = kwargs.get('CompareSubcolumnsWith', None)
        self.comment = kwargs.get('Desc', None)

        self.column_metadata = []
        self.row_cache_size = None
        self.key_cache_size = None
        self.read_repair_chance = None
        self.gc_grace_seconds = None
        self.default_validation_class = None
        self.id = None
        self.min_compaction_threshold = None
        self.max_compaction_threshold = None
        self.row_cache_save_period_in_seconds = None
        self.key_cache_save_period_in_seconds = None
        self.memtable_flush_after_mins = None
        self.memtable_throughput_in_mb = None
        self.memtable_operations_in_millions = None

class KsDef(object):

    def __init__(self, name, cf_defs):
        self.name = name
        self.cf_defs = cf_defs

        self.replication_factor = None
        self.strategy_class = None
        self.strategy_options = None

class AuthenticationException(parent.AuthenticationException):
    pass

class AuthorizationException(parent.AuthorizationException):
    pass

class AuthenticationRequest(parent.AuthenticationRequest):
    pass
