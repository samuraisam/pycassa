import pycassa.cassandra07.ttypes as parent

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
                nfe = NotFoundException()
                nfe.why = 'Column family %s not found.' % self.cf.column_family
                raise nfe
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

    def get_count(self, *args, **kwargs):
        try:
            self._obtain_connection()
            return self._tlocal.client.get_count(*args, **kwargs)
        finally:
            self._release_connection()

    def insert(self, key, col, value, timestamp, wcl, supercol=None, ttl=None):
        column = Column(col, value, timestamp, ttl)
        cp = ColumnParent(self.cf.column_family, supercol)
        try:
            self._obtain_connection()
            return self._tlocal.client.insert(key, cp, column, wcl)
        finally:
            self._release_connection()

class Column(parent.Column):

    def __init__(self, name, value, timestamp, ttl=None):
        parent.Column.__init__(self, name, value, timestamp, ttl)

class SuperColumn(parent.SuperColumn):
    pass

class ColumnOrSuperColumn(parent.ColumnOrSuperColumn):
    pass

class ColumnDef(parent.ColumnDef):

    def __init__(self, name, validation_class=None, index_type=None, index_name=None):
        if index_type is not None:
            index_type = getattr(parent.IndexType, index_type)
        parent.ColumnDef.__init__(self, name, validation_class, index_type, index_name)

class ColumnParent(parent.ColumnParent):
    pass

class ColumnPath(parent.ColumnPath):
    pass

class IndexExpression(parent.IndexExpression):

    def __init__(self, name, op, value):
        op = getattr(parent.IndexOperator, op)
        parent.IndexExpression.__init__(self, name, op, value)

class IndexClause(parent.IndexClause):
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

class KeyCount(parent.KeyCount):
    pass

class CfDef(parent.CfDef):
    pass

class KsDef(parent.KsDef):
    pass

class AuthenticationException(parent.AuthenticationException):
    pass

class AuthorizationException(parent.AuthorizationException):
    pass

class AuthenticationRequest(parent.AuthenticationRequest):
    pass
