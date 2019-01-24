# Copyright 2013-2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from binascii import unhexlify
import datetime
import tempfile
import six
import time

import cassandra
from cassoldra.cqltypes import (BooleanType, lookup_casstype_simple, lookup_casstype,
                                LongType, DecimalType, SetType, cql_typename,
                                CassandraType, UTF8Type, parse_casstype_args,
                                SimpleDateType, TimeType, ByteType, ShortType,
                                EmptyValue, _CassandraType, DateType, int64_pack)
from cassoldra.encoder import cql_quote
from cassoldra.protocol import (write_string, read_longstring, write_stringmap,
                                read_stringmap, read_inet, write_inet,
                                read_string, write_longstring)
from cassoldra.query import named_tuple_factory


class TypeTests(unittest.TestCase):

    def test_lookup_casstype_simple(self):
        """
        Ensure lookup_casstype_simple returns the correct classes
        """

        self.assertEqual(lookup_casstype_simple('AsciiType'), cassoldra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype_simple('LongType'), cassoldra.cqltypes.LongType)
        self.assertEqual(lookup_casstype_simple('BytesType'), cassoldra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype_simple('BooleanType'), cassoldra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype_simple('CounterColumnType'), cassoldra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype_simple('DecimalType'), cassoldra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype_simple('DoubleType'), cassoldra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype_simple('FloatType'), cassoldra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype_simple('InetAddressType'), cassoldra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype_simple('Int32Type'), cassoldra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype_simple('UTF8Type'), cassoldra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype_simple('DateType'), cassoldra.cqltypes.DateType)
        self.assertEqual(lookup_casstype_simple('SimpleDateType'), cassoldra.cqltypes.SimpleDateType)
        self.assertEqual(lookup_casstype_simple('ByteType'), cassoldra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype_simple('ShortType'), cassoldra.cqltypes.ShortType)
        self.assertEqual(lookup_casstype_simple('TimeUUIDType'), cassoldra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype_simple('TimeType'), cassoldra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype_simple('UUIDType'), cassoldra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype_simple('IntegerType'), cassoldra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype_simple('MapType'), cassoldra.cqltypes.MapType)
        self.assertEqual(lookup_casstype_simple('ListType'), cassoldra.cqltypes.ListType)
        self.assertEqual(lookup_casstype_simple('SetType'), cassoldra.cqltypes.SetType)
        self.assertEqual(lookup_casstype_simple('CompositeType'), cassoldra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype_simple('ColumnToCollectionType'), cassoldra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype_simple('ReversedType'), cassoldra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype_simple('unknown')), str(cassoldra.cqltypes.mkUnrecognizedType('unknown')))

    def test_lookup_casstype(self):
        """
        Ensure lookup_casstype returns the correct classes
        """

        self.assertEqual(lookup_casstype('AsciiType'), cassoldra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype('LongType'), cassoldra.cqltypes.LongType)
        self.assertEqual(lookup_casstype('BytesType'), cassoldra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype('BooleanType'), cassoldra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype('CounterColumnType'), cassoldra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype('DateType'), cassoldra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('DecimalType'), cassoldra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype('DoubleType'), cassoldra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype('FloatType'), cassoldra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype('InetAddressType'), cassoldra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype('Int32Type'), cassoldra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype('UTF8Type'), cassoldra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype('DateType'), cassoldra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('TimeType'), cassoldra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype('ByteType'), cassoldra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype('ShortType'), cassoldra.cqltypes.ShortType)
        self.assertEqual(lookup_casstype('TimeUUIDType'), cassoldra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype('UUIDType'), cassoldra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype('IntegerType'), cassoldra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype('MapType'), cassoldra.cqltypes.MapType)
        self.assertEqual(lookup_casstype('ListType'), cassoldra.cqltypes.ListType)
        self.assertEqual(lookup_casstype('SetType'), cassoldra.cqltypes.SetType)
        self.assertEqual(lookup_casstype('CompositeType'), cassoldra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype('ColumnToCollectionType'), cassoldra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype('ReversedType'), cassoldra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype('unknown')), str(cassoldra.cqltypes.mkUnrecognizedType('unknown')))

        self.assertRaises(ValueError, lookup_casstype, 'AsciiType~')

    def test_casstype_parameterized(self):
        self.assertEqual(LongType.cass_parameterized_type_with(()), 'LongType')
        self.assertEqual(LongType.cass_parameterized_type_with((), full=True), 'org.apache.cassoldra.db.marshal.LongType')
        self.assertEqual(SetType.cass_parameterized_type_with([DecimalType], full=True), 'org.apache.cassoldra.db.marshal.SetType(org.apache.cassoldra.db.marshal.DecimalType)')

        self.assertEqual(LongType.cql_parameterized_type(), 'bigint')

        subtypes = (cassoldra.cqltypes.UTF8Type, cassoldra.cqltypes.UTF8Type)
        self.assertEqual('map<text, text>',
                         cassoldra.cqltypes.MapType.apply_parameters(subtypes).cql_parameterized_type())

    def test_datetype_from_string(self):
        # Ensure all formats can be parsed, without exception
        for format in cassoldra.cqltypes.cql_timestamp_formats:
            date_string = str(datetime.datetime.now().strftime(format))
            cassoldra.cqltypes.DateType.interpret_datestring(date_string)

    def test_cql_typename(self):
        """
        Smoke test cql_typename
        """

        self.assertEqual(cql_typename('DateType'), 'timestamp')
        self.assertEqual(cql_typename('org.apache.cassoldra.db.marshal.ListType(IntegerType)'), 'list<varint>')

    def test_named_tuple_colname_substitution(self):
        colnames = ("func(abc)", "[applied]", "func(func(abc))", "foo_bar", "foo_bar_")
        rows = [(1, 2, 3, 4, 5)]
        result = named_tuple_factory(colnames, rows)[0]
        self.assertEqual(result[0], result.func_abc)
        self.assertEqual(result[1], result.applied)
        self.assertEqual(result[2], result.func_func_abc)
        self.assertEqual(result[3], result.foo_bar)
        self.assertEqual(result[4], result.foo_bar_)

    def test_parse_casstype_args(self):
        class FooType(CassandraType):
            typename = 'org.apache.cassoldra.db.marshal.FooType'

            def __init__(self, subtypes, names):
                self.subtypes = subtypes
                self.names = names

            @classmethod
            def apply_parameters(cls, subtypes, names):
                return cls(subtypes, [unhexlify(six.b(name)) if name is not None else name for name in names])

        class BarType(FooType):
            typename = 'org.apache.cassoldra.db.marshal.BarType'

        ctype = parse_casstype_args(''.join((
            'org.apache.cassoldra.db.marshal.FooType(',
                '63697479:org.apache.cassoldra.db.marshal.UTF8Type,',
                'BarType(61646472657373:org.apache.cassoldra.db.marshal.UTF8Type),',
                '7a6970:org.apache.cassoldra.db.marshal.UTF8Type',
            ')')))

        self.assertEqual(FooType, ctype.__class__)

        self.assertEqual(UTF8Type, ctype.subtypes[0])

        # middle subtype should be a BarType instance with its own subtypes and names
        self.assertIsInstance(ctype.subtypes[1], BarType)
        self.assertEqual([UTF8Type], ctype.subtypes[1].subtypes)
        self.assertEqual([b"address"], ctype.subtypes[1].names)

        self.assertEqual(UTF8Type, ctype.subtypes[2])
        self.assertEqual([b'city', None, b'zip'], ctype.names)

    def test_empty_value(self):
        self.assertEqual(str(EmptyValue()), 'EMPTY')

    def test_datetype(self):
        now_time_seconds = time.time()
        now_datetime = datetime.datetime.utcfromtimestamp(now_time_seconds)

        # Cassandra timestamps in millis
        now_timestamp = now_time_seconds * 1e3

        # same results serialized
        self.assertEqual(DateType.serialize(now_datetime, 0), DateType.serialize(now_timestamp, 0))

        # deserialize
        # epoc
        expected = 0
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime.utcfromtimestamp(expected))

        # beyond 32b
        expected = 2 ** 33
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(2242, 3, 16, 12, 56, 32))

        # less than epoc (PYTHON-119)
        expected = -770172256
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(1945, 8, 5, 23, 15, 44))

        # work around rounding difference among Python versions (PYTHON-230)
        expected = 1424817268.274
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000))

    def test_write_read_string(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_string(f, value)
            f.seek(0)
            self.assertEqual(read_string(f), value)

    def test_write_read_longstring(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_longstring(f, value)
            f.seek(0)
            self.assertEqual(read_longstring(f), value)

    def test_write_read_stringmap(self):
        with tempfile.TemporaryFile() as f:
            value = {'key': 'value'}
            write_stringmap(f, value)
            f.seek(0)
            self.assertEqual(read_stringmap(f), value)

    def test_write_read_inet(self):
        with tempfile.TemporaryFile() as f:
            value = ('192.168.1.1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

        with tempfile.TemporaryFile() as f:
            value = ('2001:db8:0:f101::1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

    def test_cql_quote(self):
        self.assertEqual(cql_quote(u'test'), "'test'")
        self.assertEqual(cql_quote('test'), "'test'")
        self.assertEqual(cql_quote(0), '0')
