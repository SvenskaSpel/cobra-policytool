from __future__ import print_function
import csv
import time
from atlas import AtlasError


def strip_qualified_name(qualified_name):
    return qualified_name.split('@')[0]


def read_file(file_path):
    with open(file_path) as f:
        data = list(csv.DictReader(f, delimiter=';'))
    return data


def add_environment(data, environment):
    for record in data:
        record['schema'] = record['schema'] + '_' + environment
    return data


def print_sync_worklog(log):
    for k in log:
        print(k+": "+", ".join(log[k]))


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
    result={}
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


class Sync:
    """
    This class is not thread safe.
    """

    worklog = {}

    def __init__(self, atlas_client, retries=0, retry_delay=60):
        self.atlas_client = atlas_client
        self.retries = retries
        self.retry_delay = retry_delay

    def sync_table_tags(self, src_table_tags):
        """
        :param src_table_tags: Array of dicts with keys (schema, table, tags (comma separated in string))
        :return: Dictionary with actions as keys and metadata as value.
        """
        self.worklog = {}
        run = 0
        while True:
            try:
                run += 1
                return self._sync_table_tags(src_table_tags, run)
            except (SyncError, IOError, AtlasError) as e:
                if run > self.retries:
                    raise e
                time.sleep(self.retry_delay)

    def _sync_table_tags(self, src_table_tags, run):
        # Verify Atlas knows about all tags used.

        src_tags = tags_from_src(src_table_tags)
        atlas_tags = self.tags_from_atlas()
        missing_atlas_tags = src_tags-atlas_tags
        if len(missing_atlas_tags) != 0:
            self.atlas_client.add_tag_definitions(missing_atlas_tags)
            self.worklog['run:%s New tags added to Atlas from tables tag file' % run] = missing_atlas_tags

        # Get all tables for schemas from atlas. Verify all exists. (both directions)
        schemas = schemas_from_src(src_table_tags)
        src_tables = tables_from_src(src_table_tags)
        atlas_tables = self.tables_from_atlas(schemas)
        if len(src_tables-set(atlas_tables.keys())) != 0:
            raise SyncError("run:%s The table(s) %s does not exist in Atlas." % (run, ", ".join(src_tables-set(atlas_tables.keys()))))
        if len(set(atlas_tables.keys())-src_tables) != 0:
            self.worklog['run:%s tables not existing in tags file' % (run)] = set(atlas_tables.keys())-src_tables
            
        # Remove tables that does not exists in Atlas
        # For each table, sync tags
        for s in src_table_tags:
            expected_tags = set(s['tags'].split(','))-set([''])
            table_name = s['schema']+"."+s['table']
            # If table does not exists in Atlas do not apply
            if table_name in src_tables:
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

    def sync_column_tags(self, src_column_tags):
        """
        :param src_column_tags: Array of dicts with keys (schema, table, attribute, tags (comma separated in string))
        :return: Dictionary with actions as keys and metadata as value.
        """
        self.worklog = {}
        run = 0
        while True:
            try:
                run += 1
                return self._sync_column_tags(src_column_tags, run)
            except (SyncError, IOError, AtlasError) as e:
                if run > self.retries:
                    raise e
                time.sleep(self.retry_delay)

    def _sync_column_tags(self, src_column_tags, run):
        """
        :param src_column_tags: Array of dicts with keys (schema, table, attribute, tags (comma separated in string))
        :return: Dictionary with actions as keys and metadata as value.
        """
        # Verify Atlas knows about all tags used.
        src_tags = tags_from_src(src_column_tags)
        atlas_tags = self.tags_from_atlas()
        missing_atlas_tags = src_tags-atlas_tags
        if len(missing_atlas_tags) != 0:
            self.atlas_client.add_tag_definitions(missing_atlas_tags)
            self.worklog['run:%s New tags added to Atlas from columns tag file' % run] = missing_atlas_tags

        # Get all columns for tables from atlas. Verify all exists. (both directions)
        src_tables = tables_from_src(src_column_tags)
        src_columns = columns_from_src(src_column_tags)
        atlas_columns = self.columns_from_atlas(src_tables)
        if len(src_columns-set(atlas_columns.keys())) != 0:
            raise SyncError("run:%s The column(s) %s does not exist in Atlas." % (run, ", ".join(src_columns-set(atlas_columns.keys()))))
        if len(set(atlas_columns.keys())-src_columns) != 0:
            self.worklog['run:%s columns not existing in tags file' % run] = set(atlas_columns.keys())-src_columns
            
        # Remove columns that does not exists in Atlas
        # For each column, sync tags
        for s in src_column_tags:
            expected_tags = set(s['tags'].split(','))-set([''])
            column_name = s['schema']+"."+s['table']+"."+s['attribute']
            # If column does not exists in Atlas do not apply
            if column_name in src_columns:
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

    def tables_from_atlas(self, schemas):
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

    def columns_from_atlas(self, src_tables):
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


class SyncError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
