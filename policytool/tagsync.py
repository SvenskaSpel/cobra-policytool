from __future__ import print_function
import csv
import time
from atlas import AtlasError
from hive import HiveError


def strip_qualified_name(qualified_name):
    return qualified_name.split('@')[0]


def read_file(file_path):
    with open(file_path, 'rU') as f:
        data = list(csv.DictReader(f, delimiter=';'))
    return data


def add_environment(data, environment):
    for record in data:
        record['schema'] = record['schema'] + '_' + environment
    return data


def print_sync_worklog(log):
    for k in log:
        print(k+": "+"\n\t".join(log[k]))


def tags_from_src(src_data):
    """
    :param src_data: A source data file (either column or table)
    :return: Set of all tags found in the file.
    """
    return set([tag for tags in src_data for tag in tags['tags'].split(',')])-set([''])


def schemas_from_src(src_data):
    """
    :param src_data: A source data file (either column or table)
    :return: All schemas found in file.
    """
    return set([s['schema'] for s in src_data])


def tables_from_src(src_data):
    """
    :param src_data: A source data file (either column or table)
    :return: All tables found in file prefixed with schema.
    """
    return set([s['schema']+"."+s['table'] for s in src_data])


def columns_from_src(src_data):
    """
    :param src_data: A source data file with columns.
    :return: All columns found in file prefixed with schema.table.
    """
    return set([s['schema']+"."+s['table']+"."+s['attribute'] for s in src_data])


def diff_table_tags(src_data_tables, atlas_tables):
    """
    :param src_data_tables: Source data with all tables.
    :param atlas_tables: Atlas tables retrieved with Sync.tables_from_atlas()
    :return: Dict of tables where value is (tags only in src, tags only in atlas)
    """
    result = {}
    for s in src_data_tables:
        expected_tags = set(s['tags'].split(','))-set([''])
        table_name = s['schema']+"."+s['table']
        if atlas_tables.has_key(table_name):
            atlas_table_tags = atlas_tables[table_name]['tags']
        else:
            atlas_table_tags = set()
        tags_only_in_src = expected_tags-atlas_table_tags
        tags_only_in_atlas = atlas_table_tags-expected_tags
        result[table_name] = (tags_only_in_src, tags_only_in_atlas)
    return result


def diff_column_tags(src_data_columns, atlas_columns):
    """
    :param src_data_columns: Source data with all columns.
    :param atlas_columns: Atlas columns retrieved with Sync.columns_from_atlas()
    :return: Dict of columns where value is (tags only in src, tags only in atlas)
    """
    result = {}
    for col in src_data_columns:
        expected_tags = set(col['tags'].split(','))-set([''])
        column_name = col['schema']+"."+col['table']+"."+col['attribute']
        if atlas_columns.has_key(column_name):
            atlas_column_tags = atlas_columns[column_name]['tags']
        else:
            atlas_column_tags = set()
        tags_only_in_src = expected_tags-atlas_column_tags
        tags_only_in_atlas = atlas_column_tags-expected_tags
        result[column_name] = (tags_only_in_src, tags_only_in_atlas)
    return result


def _tags_as_set(csv_line):
    return set(csv_line['tags'].split(',')) - {''}


class Sync:
    """
    This class is not thread safe.
    """

    worklog = {}

    def __init__(self, atlas_client, retries=0, retry_delay=60, hive_client=None):
        self.atlas_client = atlas_client
        self.hive_client = hive_client
        self.retries = retries
        self.retry_delay = retry_delay

    def sync_table_tags(self, src_table_tags, clear_not_listed=False):
        """
        :param src_table_tags: Array of dicts with keys (schema, table, tags (comma separated in string))
        :param clear_not_listed: Set to true if tables only known by atlas but not in src_table_tags shall
         have it tags removed.
        :return: Dictionary with actions as keys and metadata as value, used for logging.
        """
        self.worklog = {}
        run = 0
        while True:
            try:
                run += 1
                return self._sync_table_tags(src_table_tags, run, clear_not_listed)
            except (SyncError, IOError, AtlasError) as e:
                if run > self.retries:
                    raise e
                time.sleep(self.retry_delay)

    def _sync_table_tags(self, src_table_tags, run, clear_not_listed=False):
        # Verify Atlas knows about all tags used.

        self.ensure_tags_in_atlas(src_table_tags)

        # Get all tables for schemas from atlas. Verify all exists. (both directions)
        schemas = schemas_from_src(src_table_tags)
        src_tables = tables_from_src(src_table_tags)
        atlas_tables = self.get_tables_for_schema_from_atlas(schemas)
        if len(src_tables-set(atlas_tables.keys())) != 0:
            raise SyncError("run:%s The table(s) %s does not exist in Atlas." % (run, ", ".join(src_tables-set(atlas_tables.keys()))))
        tables_only_known_by_atlas = set(atlas_tables.keys())-src_tables
        if len(tables_only_known_by_atlas) != 0:
            self.worklog['run:%s tables not existing in tags file' % run] = tables_only_known_by_atlas
            if clear_not_listed:
                for t in tables_only_known_by_atlas:
                    (schema, table) = t.split(".")
                    src_table_tags.append({'schema': schema, 'table': table, 'tags': ''})
            
        # For each table, sync tags
        for s in src_table_tags:
            expected_tags = _tags_as_set(s)
            table_name = s['schema']+"."+s['table']
            atlas_table = atlas_tables[table_name]
            tags_to_add = expected_tags-atlas_table['tags']
            tags_to_delete = atlas_table['tags']-expected_tags
            if len(tags_to_add) != 0:
                self.atlas_client.add_tags_on_guid(atlas_table['guid'], list(tags_to_add))
                self.worklog['run:%s %s added tag' % (run, table_name)] = tags_to_add
            if len(tags_to_delete) != 0:
                self.atlas_client.delete_tags_on_guid(atlas_table['guid'], list(tags_to_delete))
                self.worklog['run:%s %s deleted tag' % (run, table_name)] = tags_to_delete
        return self.worklog

    def sync_column_tags(self, src_column_tags, clear_not_listed=False):
        """
        :param src_column_tags: Array of dicts with keys (schema, table, attribute, tags (comma separated in string))
        :param clear_not_listed: Set to true if column only known by atlas but not in src_column_tags
        shall have it tags removed.
        :return: Dictionary with actions as keys and metadata as value.
        """
        self.worklog = {}
        run = 0
        while True:
            try:
                run += 1
                return self._sync_column_tags(src_column_tags, run, clear_not_listed)
            except (SyncError, IOError, AtlasError) as e:
                if run > self.retries:
                    raise e
                time.sleep(self.retry_delay)

    def _sync_column_tags(self, src_column_tags, run, clear_not_listed=False):
        """
        :param src_column_tags: Array of dicts with keys (schema, table, attribute, tags (comma separated in string))
        :param clear_not_listed: Set to true if column only known by atlas but not in src_column_tags
        shall have it tags removed.
        :return: Dictionary with actions as keys and metadata as value, used for logging.
        """

        self.ensure_tags_in_atlas(src_column_tags)

        # Get all columns for tables from atlas. Verify all exists. (both directions)
        src_tables = tables_from_src(src_column_tags)
        src_columns = columns_from_src(src_column_tags)
        atlas_columns = self.get_columns_for_tables_from_atlas(src_tables)
        if len(src_columns-set(atlas_columns.keys())) != 0:
            raise SyncError("run:%s The column(s) %s does not exist in Atlas." % (run, ", ".join(src_columns-set(atlas_columns.keys()))))
        columns_only_known_by_atlas = set(atlas_columns.keys())-src_columns
        if len(columns_only_known_by_atlas) != 0:
            self.worklog['run:%s columns not existing in tags file' % run] = columns_only_known_by_atlas
            if clear_not_listed:
                for t in columns_only_known_by_atlas:
                    (schema, table, attribute) = t.split(".")
                    src_column_tags.append({'schema': schema, 'table': table, 'attribute': attribute, 'tags': ''})
            
        # Remove columns that does not exists in Atlas
        # For each column, sync tags
        for s in src_column_tags:
            expected_tags = _tags_as_set(s)
            column_name = s['schema']+"."+s['table']+"."+s['attribute']
            atlas_column = atlas_columns[column_name]
            tags_to_add = expected_tags-atlas_column['tags']
            tags_to_delete = atlas_column['tags']-expected_tags
            if len(tags_to_add) != 0:
                self.atlas_client.add_tags_on_guid(atlas_column['guid'], list(tags_to_add))
                self.worklog['run:%s %s added tag' % (run, column_name)] = tags_to_add
            if len(tags_to_delete) != 0:
                self.atlas_client.delete_tags_on_guid(atlas_column['guid'], list(tags_to_delete))
                self.worklog['run:%s %s deleted tag' % (run, column_name)] = tags_to_delete
        return self.worklog

    def tags_from_atlas(self):
        return set([t['name'] for t in self.atlas_client.known_tags()])

    def ensure_tags_in_atlas(self, csv_dict):
        src_tags = tags_from_src(csv_dict)
        atlas_tags = self.tags_from_atlas()
        missing_atlas_tags = src_tags - atlas_tags
        if len(missing_atlas_tags) != 0:
            self.atlas_client.add_tag_definitions(missing_atlas_tags)
            self.worklog['New tags added to Atlas'] = missing_atlas_tags

    def get_tables_for_schema_from_atlas(self, schemas):
        """
        :param schemas:
        :return: {'schema.table': {'guid':, 'tags': []}, ...
        """
        result={}
        for schema in schemas:
            for table in self.atlas_client.get_tables(schema):
                result[strip_qualified_name(table['attributes']['qualifiedName'])]={
                    'guid': table['guid'],
                    'tags': set(table['classificationNames'])}
        return result

    def get_columns_for_tables_from_atlas(self, src_tables):
        """
        :param src_tables: ['schema.table1', 'schema.table2' ...]:
        :return: {'schema.table.column': {'guid':, 'tags': []}, ...
        """
        result={}
        for schema_table in src_tables:
            (schema, table)=schema_table.split(".")
            for table in self.atlas_client.get_columns(schema, table):
                result[strip_qualified_name(table['attributes']['qualifiedName'])]={
                    'guid': table['guid'],
                    'tags': set(table['classificationNames'])}
        return result

    def _sync_tags_for_one_tables_storage(self, schema, table, expected_tags):
        """
        Ensure the storage directory for table in schema has the same tags as the table.
        Location for storage is looked up in hive server.
        :param schema: Name of schema.
        :param table: Name of table.
        :param expected_tags: List of strings with expected tags.
        :return: Dictionary with actions as keys and metadata as value, used for logging.
        """
        storage_url = self.hive_client.get_location(schema, table)
        if storage_url is not None:
            guid = self.atlas_client.add_hdfs_path(storage_url)

            tags_on_storage = self.atlas_client.get_tags_on_guid(guid)
            tags_to_add = expected_tags-tags_on_storage
            tags_to_delete = tags_on_storage-expected_tags
            if len(tags_to_add) != 0:
                self.atlas_client.add_tags_on_guid(guid, list(tags_to_add))
                self.worklog['{} added tag'.format(storage_url)] = tags_to_add
            if len(tags_to_delete) != 0:
                self.atlas_client.delete_tags_on_guid(guid, list(tags_to_delete))
                self.worklog['{} deleted tag'.format(storage_url)] = tags_to_delete
        else:
            self.worklog['{}.{} is a view, not doing any hdfs tagging for it.'.format(schema, table)] = ''
        return self.worklog

    def sync_table_storage_tags(self, src_table_tags, clear_not_listed=False):
        """
        Ensure the storage directories has the same tags as specified for the table in src_table_tags.
        Location for storage is looked up in hive server.
        :param src_table_tags: Array of dicts with keys (schema, table, tags (comma separated in string))
        :param clear_not_listed: Set to true, then we will look up tables in Atlas and also clear tags on
        those tables listed there. Only clear tags on schemas listed at least once in src_table_tags.
        :return: Dictionary with actions as keys and metadata as value, used for logging.
        """
        self.worklog = {}
        run = 0
        while True:
            try:
                run += 1
                self.ensure_tags_in_atlas(src_table_tags)
                if clear_not_listed:
                    schemas = schemas_from_src(src_table_tags)
                    src_tables = tables_from_src(src_table_tags)
                    atlas_tables = self.get_tables_for_schema_from_atlas(schemas)
                    tables_only_known_by_atlas = set(atlas_tables.keys())-src_tables
                    if len(tables_only_known_by_atlas) != 0:
                        for t in tables_only_known_by_atlas:
                            (schema, table) = t.split(".")
                            src_table_tags.append({'schema': schema, 'table': table, 'tags': ''})
                for s in src_table_tags:
                    self.worklog.update(
                        self._sync_tags_for_one_tables_storage(s['schema'], s['table'], _tags_as_set(s)))
                return self.worklog
            except (SyncError, IOError, AtlasError, HiveError) as e:
                if run > self.retries:
                    raise e
                time.sleep(self.retry_delay)


class SyncError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
