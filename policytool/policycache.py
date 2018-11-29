import csv
import json
import os
from collections import Counter


# TODO:
# * get policycache via API request
# * HDFS, handle resources not known.
# Add CLI.

from requests_kerberos import HTTPKerberosAuth

import atlas
import hive
import tagsync


class PolicyCache:

    def __init__(self, cache_json_dict):
        self.tags = cache_json_dict['tags']
        self.dbResources = PolicyCache._extract_resources(cache_json_dict, 'db')
        self.tableResources = PolicyCache._extract_resources(cache_json_dict, 'table')
        self.columnResources = PolicyCache._extract_resources(cache_json_dict, 'column')
        self.resourceTagMapping = cache_json_dict['resourceToTagIds']

    @classmethod
    def _extract_resources(cls, policy_cache_dict, resource):
        expected_resource_elements = {'db': [u'database'],
                                      'table': [u'database', u'table'],
                                      'column': [u'database', u'table', u'column']}[resource]
        expected_counter = Counter(expected_resource_elements)
        result = {}
        for res in policy_cache_dict['serviceResources']:
            if Counter(res['resourceElements'].keys()) == expected_counter:
                qualified_key = []
                for k in expected_resource_elements:
                    qualified_key.append(res['resourceElements'][k]['values'][0])
                result[tuple(qualified_key)] = res['id']
        return result

    def _tags_for_resource(self, resource_id):
        tag_ids = self.resourceTagMapping[str(resource_id)]
        tag_names = [str(self.tags[str(tag_id)]['type']) for tag_id in tag_ids]
        return tag_names

    def get_tags_for_all_tables(self):
        # Build structure of tableResources to tags:
        # Return structure of [{'schema': schema, 'table':table, 'attribute': col, 'tags': tags}, ...]
        # tags as comma separated string to make structure useful for csv.
        return [{'schema': resource[0], 'table': resource[1], 'tags': ','.join(self._tags_for_resource(self.tableResources[resource]))} for resource in self.tableResources]

    def get_tags_for_all_columns(self):
        # Build structure of tableResources to tags:
        # Return structure of {'FQRN': {'schema': schema, 'table':table, 'attribute': col, 'tags': tags}
        # tags as comma separated string to make structure useful for csv.
        return [{'schema': resource[0], 'table': resource[1], 'attribute': resource[2], 'tags': ','.join(self._tags_for_resource(self.columnResources[resource]))} for resource in self.columnResources]

    def get_tags_for_all_databases(self):
        # Build structure of tableResources to tags:
        # Return structure of {'FQRN': {'schema': schema, 'table':table, 'attribute': col, 'tags': tags}
        # tags as comma separated string to make structure useful for csv.
        return [{'schema': resource[0], 'tags': ','.join(self._tags_for_resource(self.dbResources[resource]))}
                for resource in self.dbResources]


def extract_policy_cache(config, policy_cache_file=None, table_tag_file=None, column_tag_file=None, hdfs=False, ignore_list=[]):
    """
    Functionality to sync Rangers view of tags with Atlas. Useful when Atlas database or the Kafka topic
    ranger_entities_consumer has been dropped.
    Reads a policycahe files with tags copied from Hives policy cache. Either output table and column tag files if those
    parameters provided or else talk directly to Atlas and sync so Atlas is equal to the policy cache.
    :param config: Config for the environment to talk to.
    :param policy_cache_file: Input file, a policy cache file with tags from Hive.
    :param table_tag_file: Output file for tags on tables or None if not desired.
    :param column_tag_file: Output file for tags on columns or None if not desired.
    :param hdfs: Set to true if also sync to HDFS paths. Not used if tag-files are set.
    :param ignore_list: List of 'schema.table' to ignore.
    :return: None
    """
    policy_cache_data = json.load(open(policy_cache_file))
    policy_cache = PolicyCache(policy_cache_data)
    tables_dict = _remove_ignores(policy_cache.get_tags_for_all_tables(), ignore_list)
    columns_dict = _remove_ignores(policy_cache.get_tags_for_all_columns(), ignore_list)
    if table_tag_file is None and column_tag_file is None:
        atlas_client = atlas.Client(config['atlas_api_url'], auth=HTTPKerberosAuth())
        hive_client = None
        if hdfs:
            hive_client = hive.Client(config['hive_server'], config['hive_port'])
        sync_client = tagsync.Sync(atlas_client, hive_client=hive_client)
        sync_client.sync_table_tags(tables_dict, clear_not_listed=True)
        sync_client.sync_column_tags(columns_dict, clear_not_listed=True)
        if hdfs:
            sync_client.sync_table_storage_tags(tables_dict, clear_not_listed=True)
    elif table_tag_file and column_tag_file:
        _write_table_tag_file(table_tag_file, tables_dict)
        _write_column_tag_file(column_tag_file, columns_dict)
    else:
        raise AttributeError("Either both table tag and column tag files must be set or neither.")


def _remove_ignores(table_dict, ignore_list):
    result=[]
    for t in table_dict:
        fq_table = t['schema'] + '.' + t['table']
        if fq_table not in ignore_list:
            result.append(t)
    return result


def _write_table_tag_file(table_tag_file, tables_dict):
    with open(table_tag_file, 'wb') as csv_file:
        table_writer = csv.DictWriter(
            csv_file, fieldnames=['schema', 'table', 'tags'], delimiter=';', lineterminator=os.linesep)
        table_writer.writeheader()
        table_writer.writerows(tables_dict)


def _write_column_tag_file(column_tag_file, columns_dict):
    with open(column_tag_file, 'wb') as csv_file:
        table_writer = csv.DictWriter(
            csv_file, fieldnames=['schema', 'table', 'attribute', 'tags'], delimiter=';', lineterminator=os.linesep)
        table_writer.writeheader()
        table_writer.writerows(columns_dict)
