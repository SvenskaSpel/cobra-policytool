import unittest
from policytool import tagsync
from policytool.atlas import AtlasError


class TestTagsyncModuleGlobal(unittest.TestCase):

    def test_strip_qualified_name(self):
        self.assertEqual(tagsync.strip_qualified_name("foo@bar"), 'foo')

    def test_tags_from_src(self):
        test_data = [{'tags': 'tag1'},
                     {'tags': 'tag1,tag2,tag3'}]
        self.assertEqual(tagsync.tags_from_src(test_data), set(['tag1', 'tag2', 'tag3']))


class TestSync(unittest.TestCase):

    def setUp(self):
        self.atlas_client = type('atlas_client', (), {})()
        self.to_test = tagsync.Sync(self.atlas_client, retries=2, retry_delay=1)

    def test_sync_table_tags_expekt_tags_added_to_one_table(self):
        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': [u'tag']},
                                                   {u'status': u'ACTIVE',
                                                    u'guid': u'UUID2',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table2',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table2@dhadoopname',
                                                        u'name': u'table2',
                                                        u'description': None},
                                                    u'classificationNames': []}]
        self.atlas_client.add_tags_on_guid = lambda guid, tags: added_tags.append((guid, tags))

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'tags': 'tag'},
                     {'schema': 'test_schema',
                      'table': 'table2',
                      'tags': 'tag'}]
        result = self.to_test.sync_table_tags(test_data)

        self.assertEqual([(u'UUID2', ['tag'])], added_tags)
        self.assertEqual({'run:1 test_schema.table2 added tag': set(['tag'])}, result)

    def test_sync_table_tags_expekt_tags_added_to_no_table(self):
        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': [u'tag']}]
        self.atlas_client.add_tags_on_guid = lambda guid, tags: added_tags.append((guid, tags))

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'tags': 'tag'}]
        result = self.to_test.sync_table_tags(test_data)

        self.assertEqual([], added_tags)
        self.assertEqual({}, result)

    def test_sync_table_tags_expekt_table_missing_in_atlas(self):
        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: []
        self.atlas_client.add_tags_on_guid = lambda guid, tags: added_tags.append((guid, tags))

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'tags': 'tag'}]
        with self.assertRaises(tagsync.SyncError) as e:
            self.to_test.sync_table_tags(test_data)
        self.assertEqual('run:3 The table(s) test_schema.table1 does not exist in Atlas.', e.exception.message)

    def test_sync_table_tags_expekt_table_guid_exist_third_run(self):
        runs = [] # Length of runs is the counter since Python 2 cannot mutate the variable itself.

        def add_tags_on_guid(guid, tags):
            runs.append(1)
            if len(runs) > 2:
                added_tags.append((guid, tags))
            else:
                raise AtlasError('I am a teapot', 418)

        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': []}]
        self.atlas_client.add_tags_on_guid = add_tags_on_guid

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'tags': 'tag'}]
        result = self.to_test.sync_table_tags(test_data)

        self.assertEqual([(u'UUID1', ['tag'])], added_tags)
        self.assertEqual({'run:3 test_schema.table1 added tag': set(['tag'])}, result)
        self.assertEqual(3, len(runs))

    def test_sync_column_tags_expext_tags_added_to_one_column(self):
        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': []}]
        self.atlas_client.get_columns = lambda db, table: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_column',
                                                    u'displayText': u'column1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.' + table + u'.column1@dhadoopname',
                                                        u'name': u'column1',
                                                        u'description': None},
                                                    u'classificationNames': [u'tag']},
                                                   {u'status': u'ACTIVE',
                                                    u'guid': u'UUID2',
                                                    u'typeName': u'hive_column',
                                                    u'displayText': u'column2',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.' + table + u'.column2@dhadoopname',
                                                        u'name': u'column2',
                                                        u'description': None},
                                                    u'classificationNames': []},
                                                    {u'status': u'ACTIVE',
                                                     u'guid': u'UUID2',
                                                     u'typeName': u'hive_column',
                                                     u'displayText': u'column3',
                                                     u'attributes': {
                                                         u'owner': u'owner',
                                                         u'qualifiedName': db + u'.' + table + u'.column3@dhadoopname',
                                                         u'name': u'column3',
                                                         u'description': None},
                                                     u'classificationNames': []}]
        self.atlas_client.add_tags_on_guid = lambda guid, tags: added_tags.append((guid, tags))

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'attribute': 'column1',
                      'tags': 'tag'},
                     {'schema': 'test_schema',
                      'table': 'table1',
                      'attribute': 'column2',
                      'tags': 'tag'}]
        result = self.to_test.sync_column_tags(test_data)

        self.assertEqual([(u'UUID2', ['tag'])], added_tags)
        self.assertEqual({'run:1 columns not existing in tags file': set([u'test_schema.table1.column3']),
                          'run:1 test_schema.table1.column2 added tag': set(['tag'])},
                         result)

    def test_sync_table_tags_expekt_column_missing_in_atlas(self):
        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': []}]
        self.atlas_client.get_columns = lambda db, table: []
        self.atlas_client.add_tags_on_guid = lambda guid, tags: added_tags.append((guid, tags))

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'attribute': 'column1',
                      'tags': 'tag'}]
        with self.assertRaises(tagsync.SyncError) as e:
            self.to_test.sync_column_tags(test_data)
        self.assertEqual('run:3 The column(s) test_schema.table1.column1 does not exist in Atlas.', e.exception.message)

    def test_sync_column_tags_expekt_column_guid_exist_third_run(self):
        runs = [] # Length of runs is the counter since Python 2 cannot mutate the variable itself.

        def add_tags_on_guid(guid, tags):
            runs.append(1)
            if len(runs) > 2:
                added_tags.append((guid, tags))
            else:
                raise AtlasError('I am a teapot', 418)

        added_tags=[]
        self.atlas_client.known_tags = lambda : [{'name': 'tag'}]
        self.atlas_client.get_tables = lambda db: [{u'status': u'ACTIVE',
                                                    u'guid': u'UUID1',
                                                    u'typeName': u'hive_table',
                                                    u'displayText': u'table1',
                                                    u'attributes': {
                                                        u'owner': u'owner',
                                                        u'qualifiedName': db + u'.table1@dhadoopname',
                                                        u'name': u'table1',
                                                        u'description': None},
                                                    u'classificationNames': []}]
        self.atlas_client.get_columns = lambda db, table: [{u'status': u'ACTIVE',
                                                            u'guid': u'UUID1',
                                                            u'typeName': u'hive_column',
                                                            u'displayText': u'column1',
                                                            u'attributes': {
                                                                u'owner': u'owner',
                                                                u'qualifiedName': db + u'.' + table + u'.column1@dhadoopname',
                                                                u'name': u'column1',
                                                                u'description': None},
                                                            u'classificationNames': []}]
        self.atlas_client.add_tags_on_guid = add_tags_on_guid

        test_data = [{'schema': 'test_schema',
                      'table': 'table1',
                      'attribute': 'column1',
                      'tags': 'tag'}]
        result = self.to_test.sync_column_tags(test_data)

        self.assertEqual([(u'UUID1', ['tag'])], added_tags)
        self.assertEqual({'run:3 test_schema.table1.column1 added tag': set(['tag'])}, result)
        self.assertEqual(3, len(runs))

if __name__ == '__main__':
    unittest.main()