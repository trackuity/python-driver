# Copyright 2015 DataStax, Inc.
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

from unittest import TestCase
from cassoldra.cqlengine.statements import DeleteStatement, WhereClause, MapDeleteClause
from cassoldra.cqlengine.operators import *
import six

class DeleteStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ds = DeleteStatement('table', 'field')
        self.assertEqual(len(ds.fields), 1)
        self.assertEqual(ds.fields[0].field, 'field')

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ds = DeleteStatement('table', ['f1', 'f2'])
        self.assertTrue(six.text_type(ds).startswith('DELETE "f1", "f2"'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE "f1", "f2"'), str(ds))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ds = DeleteStatement('table', None)
        self.assertTrue(six.text_type(ds).startswith('DELETE FROM'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM'), str(ds))

    def test_table_rendering(self):
        ds = DeleteStatement('table', None)
        self.assertTrue(six.text_type(ds).startswith('DELETE FROM table'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM table'), str(ds))

    def test_where_clause_rendering(self):
        ds = DeleteStatement('table', None)
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "a" = %(0)s', six.text_type(ds))

    def test_context_update(self):
        ds = DeleteStatement('table', None)
        ds.add_field(MapDeleteClause('d', {1: 2}, {1:2, 3: 4}))
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))

        ds.update_context_id(7)
        self.assertEqual(six.text_type(ds), 'DELETE "d"[%(8)s] FROM table WHERE "a" = %(7)s')
        self.assertEqual(ds.get_context(), {'7': 'b', '8': 3})

    def test_context(self):
        ds = DeleteStatement('table', None)
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ds.get_context(), {'0': 'b'})
