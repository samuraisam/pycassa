from pycassa import ColumnFamily, ConsistencyLevel, ConnectionPool,\
                    NotFoundException, SystemManager

from nose.tools import assert_raises, assert_equal, assert_true
from nose.plugins.skip import *

import unittest

TEST_KS = 'PycassaTestKeyspace'

def setup_module():
    global pool, cf, scf, sys_man
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace=TEST_KS, credentials=credentials, framed_transport=False)
    cf = ColumnFamily(pool, 'Standard1', dict_class=dict)
    scf = ColumnFamily(pool, 'Super1', dict_class=dict)
    sys_man = SystemManager(framed_transport=False)

def teardown_module():
    pool.dispose()

class TestColumnFamily(unittest.TestCase):

    def tearDown(self):
        for key, columns in cf.get_range():
            cf.remove(key)

    def test_empty(self):
        key = 'TestColumnFamily.test_empty'
        assert_raises(NotFoundException, cf.get, key)
        assert_equal(len(cf.multiget([key])), 0)
        for key, columns in cf.get_range():
            assert_equal(len(columns), 0)

    def test_insert_get(self):
        key = 'TestColumnFamily.test_insert_get'
        columns = {'1': 'val1', '2': 'val2'}
        assert_raises(NotFoundException, cf.get, key)
        cf.insert(key, columns)
        assert_equal(cf.get(key), columns)

    def test_insert_multiget(self):
        key1 = 'TestColumnFamily.test_insert_multiget1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'test_insert_multiget1'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        cf.insert(key1, columns1)
        cf.insert(key2, columns2)
        rows = cf.multiget([key1, key2, missing_key])
        assert_equal(len(rows), 2)
        assert_equal(rows[key1], columns1)
        assert_equal(rows[key2], columns2)
        assert_true(missing_key not in rows)

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)
        assert_equal(cf.get_count(key), 2)

    def test_insert_get_range(self):
        if sys_man.describe_partitioner() == 'RandomPartitioner':
            raise SkipTest('Cannot use RandomPartitioner for this test')

        keys = ['TestColumnFamily.test_insert_get_range%s' % i for i in xrange(5)]
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            cf.insert(key, columns)

        rows = list(cf.get_range(start=keys[0], finish=keys[-1]))
        assert_equal(len(rows), len(keys))
        for i, (k, c) in enumerate(rows):
            assert_equal(k, keys[i])
            assert_equal(c, columns)

    def test_get_range_batching(self):
        if sys_man.describe_partitioner() == 'RandomPartitioner':
            raise SkipTest('Cannot use RandomPartitioner for this test')

        for key, columns in cf.get_range():
            cf.remove(key)

        keys = []
        columns = {'c': 'v'}
        for i in range(100, 201):
            keys.append('key%d' % i)
            cf.insert('key%d' % i, columns)

        for i in range(201, 301):
            cf.insert('key%d' % i, columns)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=10):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=1000):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=150):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=7):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=2):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        # Put the remaining keys in our list
        for i in range(201, 301):
            keys.append('key%d' % i)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=2):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=7):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=200):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=10000):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        # Don't give a row count
        count = 0
        for (k,v) in cf.get_range(buffer_size=2):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=77):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=200):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=10000):
            if len(v) > 0:
                assert_true(k in keys, 'key "%s" should be in keys' % k)
                count += 1
        assert_equal(count, 201)

        for key, columns in cf.get_range():
            cf.remove(key)

    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)

        cf.remove(key, columns=['2'])
        del columns['2']
        assert_equal(cf.get(key), {'1': 'val1'})

        cf.remove(key)
        assert_raises(NotFoundException, cf.get, key)

class TestSuperColumnFamily(unittest.TestCase):

    def tearDown(self):
        for key, columns in scf.get_range():
            scf.remove(key)

    def test_empty(self):
        key = 'TestSuperColumnFamily.test_empty'
        assert_raises(NotFoundException, cf.get, key)
        assert_equal(len(cf.multiget([key])), 0)
        for key, columns in cf.get_range():
            assert_equal(len(columns), 0)

    def test_get_whole_row(self):
        key = 'TestSuperColumnFamily.test_get_whole_row'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)

    def test_get_super_column(self):
        key = 'TestSuperColumnFamily.test_get_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)
        assert_equal(scf.get(key, super_column='1'), subcolumns)
        assert_equal(scf.get(key, super_column='1', columns=['sub1']),     {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_start='sub3'),  {'sub3': 'val3'})
        assert_equal(scf.get(key, super_column='1', column_finish='sub1'), {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_count=1),       {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_count=1, column_reversed=True), {'sub3': 'val3'})

    def test_get_super_columns(self):
        key = 'TestSuperColumnFamily.test_get_super_columns'
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)
        assert_equal(scf.get(key, columns=['1']),     {'1': super1})
        assert_equal(scf.get(key, column_start='3'),  {'3': super3})
        assert_equal(scf.get(key, column_finish='1'), {'1': super1})
        assert_equal(scf.get(key, column_count=1),    {'1': super1})
        assert_equal(scf.get(key, column_count=1, column_reversed=True), {'3': super3})

    def test_multiget_supercolumn(self):
        key1 = 'TestSuerColumnFamily.test_multiget_supercolumn1'
        key2 = 'TestSuerColumnFamily.test_multiget_supercolumn2'
        keys = [key1, key2]
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key1, columns)
        scf.insert(key2, columns)

        assert_equal(scf.multiget(keys),
                     {key1: columns, key2: columns})

        assert_equal(scf.multiget(keys, super_column='1'),
                     {key1: subcolumns, key2: subcolumns})

        assert_equal(scf.multiget(keys, super_column='1', columns=['sub1']),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_start='sub3'),
                     {key1: {'sub3': 'val3'}, key2: {'sub3': 'val3'}})

        assert_equal(scf.multiget(keys, super_column='1', column_finish='sub1'),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_count=1),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_count=1, column_reversed=True),
                     {key1: {'sub3': 'val3'}, key2: {'sub3': 'val3'}})

    def test_multiget_supercolumns(self):
        key1 = 'TestSuerColumnFamily.test_multiget_supercolumns1'
        key2 = 'TestSuerColumnFamily.test_multiget_supercolumns2'
        keys = [key1, key2]
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key1, columns)
        scf.insert(key2, columns)
        assert_equal(scf.multiget(keys), {key1: columns, key2: columns})
        assert_equal(scf.multiget(keys, columns=['1']),     {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_start='3'),  {key1: {'3': super3}, key2: {'3': super3}})
        assert_equal(scf.multiget(keys, column_finish='1'), {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_count=1),    {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_count=1, column_reversed=True), {key1: {'3': super3}, key2: {'3': super3}})

    def test_get_range_super_column(self):
        key = 'TestSuperColumnFamily.test_get_range_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1')),
                     [(key, subcolumns)])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', columns=['sub1'])),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_start='sub3')),
                     [(key, {'sub3': 'val3'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_finish='sub1')),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_count=1)),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_count=1, column_reversed=True)),
                     [(key, {'sub3': 'val3'})])

    def test_get_range_super_columns(self):
        key = 'TestSuperColumnFamily.test_get_range_super_columns'
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key, columns)
        assert_equal(list(scf.get_range(start=key, finish=key, columns=['1'])),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_start='3')),
                     [(key, {'3': super3})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_finish='1')),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_count=1)),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_count=1, column_reversed=True)),
                     [(key, {'3': super3})])

    def test_get_count_super_column(self):
        key = 'TestSuperColumnFamily.test_get_count_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(scf.get_count(key), 1)
        assert_equal(scf.get_count(key, super_column='1'), 3)

    def test_batch_insert(self):
        key1 = 'TestSuperColumnFamily.test_batch_insert1'
        key2 = 'TestSuperColumnFamily.test_batch_insert2'
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2', 'sub3': 'val3'}}
        scf.batch_insert({key1: columns, key2: columns})
        assert_equal(scf.get(key1), columns)
        assert_equal(scf.get(key2), columns)

    def test_remove(self):
        key = 'TestSuperColumnFamily.test_remove'
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2', 'sub3': 'val3'}, '3': {'sub4': 'val4'}}
        scf.insert(key, columns)
        assert_equal(scf.get_count(key), 3)
        scf.remove(key, super_column='1')
        assert_equal(scf.get_count(key), 2)
        scf.remove(key, columns=['3'])
        assert_equal(scf.get_count(key), 1)

        assert_equal(scf.get_count(key, super_column='2'), 2)
        scf.remove(key, super_column='2', columns=['sub2'])
        assert_equal(scf.get_count(key, super_column='2'), 1)
