import unittest
from mock import MagicMock

from policytool import hive


class _CursorMock:

    def __init__(self, execute=lambda q: "", fetchall=lambda: []):
        self.execute_mock=execute
        self.fetchall_mock=fetchall

    def execute(self, query):
        self.execute_mock(query)

    def fetchall(self):
        return self.fetchall_mock()


class TestHiveClient(unittest.TestCase):

    def test_get_location_for_db_and_table(self):
        result_from_db=[("foo", "bar", None), ("Location:      ", "hdfs://sys/path", None)]
        to_test = hive.Client("dummyhost")
        connection_dummy = type('', (), {})()
        connection_dummy.cursor = lambda: _CursorMock(fetchall=lambda: result_from_db)
        to_test._connection = MagicMock(return_value=connection_dummy)

        result = to_test.get_location("db", "table")
        self.assertEqual(result, "hdfs://sys/path")

    def test_get_location_only_db(self):
        result_from_db=[(None, None, "hdfs://sys/path", None, None, None)]
        to_test = hive.Client("dummyhost")
        connection_dummy = type('', (), {})()
        connection_dummy.cursor = lambda: _CursorMock(fetchall=lambda: result_from_db)
        to_test._connection = MagicMock(return_value=connection_dummy)

        result = to_test.get_location("db")
        self.assertEqual(result, "hdfs://sys/path")

    def test_get_location_with_insecure_db_string(self):
        to_test = hive.Client("dummyhost")

        with self.assertRaises(hive.HiveError) as e:
            to_test.get_location("db; drop", "table")
        self.assertEqual('"db; drop" includes non allowed characters', e.exception.message)

    def test_get_location_with_insecure_table_string(self):
        to_test = hive.Client("dummyhost")

        with self.assertRaises(hive.HiveError) as e:
            to_test.get_location("db", "table; drop")
        self.assertEqual('"table; drop" includes non allowed characters', e.exception.message)

    def test_get_location_for_db_and_table_when_view_return_none(self):
        result_from_db=[("foo", "bar", None)]
        to_test = hive.Client("dummyhost")
        connection_dummy = type('', (), {})()
        connection_dummy.cursor = lambda: _CursorMock(fetchall=lambda: result_from_db)
        to_test._connection = MagicMock(return_value=connection_dummy)

        result = to_test.get_location("db", "table")
        self.assertEqual(result, None)
