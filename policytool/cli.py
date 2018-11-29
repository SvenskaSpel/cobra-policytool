# -*- coding: utf-8 -*-
from __future__ import print_function
import click
from click import ClickException
from requests_kerberos import HTTPKerberosAuth
import atlas
import hive
import policycache
import tagsync
import ranger
import rangersync
from policytool.configfile import JSONPropertiesFile
from template import Context
import os
import os.path
import json
from collections import defaultdict


SLEEP_ON_RETRY_SECONDS = 60


def _missing_files(files):
    missing = []
    for f in files:
        if not os.path.isfile(f):
            missing.append(f)
    return missing


@click.group()
def cli():
    pass


def _tags_to_atlas(srcdir, environment, hdfs, retry, verbose, config, tabletagfile, columntagfile):
    conf = JSONPropertiesFile(config).get(environment)
    table_file = os.path.join(srcdir, tabletagfile)
    column_file = os.path.join(srcdir, columntagfile)
    missing_files = _missing_files([table_file, column_file])
    if len(missing_files) != 0:
        print("Following files are missing: " + ", ".join(missing_files))
        print("Will not run, exiting!")
        return 0

    auth = HTTPKerberosAuth()
    atlas_client = atlas.Client(conf['atlas_api_url'], auth=auth)
    hive_client = None
    if hdfs:
        hive_client = hive.Client(conf['hive_server'], conf['hive_port'])
    sync_client = tagsync.Sync(atlas_client, retry*conf.get('retries', 1), SLEEP_ON_RETRY_SECONDS, hive_client)

    try:
        if verbose > 0:
            print("Syncing tags for tables.")
        src_data_table = tagsync.read_file(table_file)
        log = sync_client.sync_table_tags(tagsync.add_environment(src_data_table, environment))
        if verbose > 0:
            tagsync.print_sync_worklog(log)
            print("Syncing tags for columns.")
        src_data_column = tagsync.read_file(column_file)
        log = sync_client.sync_column_tags(tagsync.add_environment(src_data_column, environment))
        if verbose > 0:
            tagsync.print_sync_worklog(log)
        if hdfs:
            if verbose > 0:
                print("Syncing tags for table storage.")
            log = sync_client.sync_table_storage_tags(src_data_table)
            if verbose > 0:
                tagsync.print_sync_worklog(log)
    except (tagsync.SyncError, IOError) as e:
        raise ClickException(e.message + "\nTag sync not complete, fix errors and re-run.")


@cli.command("tags_to_atlas", help="sync tags from source files to Atlas.")
@click.option('-s', '--srcdir', help='The schema for the generated table', default='src/main/tags')
@click.option('-e', '--environment', help='Destination environment', required=True)
@click.option('--hdfs/--no-hdfs', help='Set tags on hive tables corresponding hdfs directory.', default=False)
@click.option('-r', '--retry', help='Retry on fail. Number of retries is controlled by \'retries\' in config.', count=True)
@click.option('-v', '--verbose', help='Provide verbose output', count=True)
@click.option('-c', '--config', help='Config file', type=click.Path(exists=True))
@click.option('--tabletagfile', help='The source file for table tags file', default='table_tags.csv')
@click.option('--columntagfile', help='The source file for column tags file', default='column_tags.csv')
def tags_to_atlas(srcdir, environment, hdfs, retry, verbose, config, tabletagfile, columntagfile):
    _tags_to_atlas(srcdir, environment, hdfs, retry, verbose, config, tabletagfile, columntagfile)


def _rules_to_ranger_cmd(srcdir, project_name, environment, config, verbose, dryrun, tabletagfile, columntagfile, policyfile):
    conf = JSONPropertiesFile(config).get(environment)
    table_file = os.path.join(srcdir, tabletagfile)
    column_file = os.path.join(srcdir, columntagfile)
    policy_file = os.path.join(srcdir, policyfile)
    missing_files = _missing_files([table_file, column_file, policy_file])
    if len(missing_files) != 0:
        print("Following files are missing: " ", ".join(missing_files))
        print("Will not run, exiting!")
        return 0

    ranger_server = conf['ranger_api_url']

    auth = HTTPKerberosAuth()
    ranger_client = ranger.Client(ranger_server, auth=auth)
    sync_client = rangersync.RangerSync(ranger_client, verbose, dryrun)

    tables = tagsync.read_file(table_file)
    columns = tagsync.read_file(column_file)

    table_columns = defaultdict(list)
    for column in columns:
        table_columns["{}.{}".format(column['schema'], column['table'])].append(column)

    context_dict = {
        "project_name": project_name,
        "environment": environment,
        "tables": tables,
        "table_columns": table_columns,
    }

    if conf.has_key('hive_server'):
        context_dict['hive_client'] = hive.Client(conf['hive_server'], conf['hive_port'])

    # Add variables from config to context_dict.
    for var in conf.get('variables', []):
        context_dict[var['name']] = var['value']

    context = Context(context_dict)

    with open(policy_file, 'rU') as f:
        policy_commands = json.load(f)

    policies = rangersync.apply_commands(policy_commands, context)
    sync_client.sync_policies([project_name + '_' + environment, 'load_etl_'], policies)


@cli.command("rules_to_ranger", help="Synchronize rules from a file to Ranger")
@click.option('-s', '--srcdir', help='The schema for the generated table', default='src/main/tags')
@click.option('-p', '--project-name', help='Project to create rules for', required=True)
@click.option('-e', '--environment', help='Destination environment', default='dev')
@click.option('-c', '--config', help='Config file', type=click.Path(exists=True))
@click.option('-v', '--verbose', help='Provide verbose output', count=True)
@click.option('--dryrun', help='Show commands, but do not update.', is_flag=True)
@click.option('--tabletagfile', help='The source file for table tags file', default='table_tags.csv')
@click.option('--columntagfile', help='The source file for column tags file', default='column_tags.csv')
@click.option('--policyfile', help='The source file for policy file', default='ranger_policies.json')
def rules_to_ranger_cmd(srcdir, project_name, environment, config, verbose, dryrun, tabletagfile, columntagfile, policyfile):
    _rules_to_ranger_cmd(srcdir, project_name, environment, config, verbose, dryrun, tabletagfile, columntagfile, policyfile)


def _audit(srcdir, environment, config, tabletagfile, columntagfile):
    conf = JSONPropertiesFile(config).get(environment)
    table_file = os.path.join(srcdir, tabletagfile)
    column_file = os.path.join(srcdir, columntagfile)
    missing_files = _missing_files([table_file, column_file])
    if len(missing_files) != 0:
        print("Following files are missing: " ", ".join(missing_files))
        print("Will not run, exiting!")
        return 0

    auth = HTTPKerberosAuth()
    atlas_client = atlas.Client(conf['atlas_api_url'], auth=auth)
    sync_client = tagsync.Sync(atlas_client)

    try:
        src_data_table = tagsync.add_environment(tagsync.read_file(table_file), environment)
        src_data_column = tagsync.add_environment(tagsync.read_file(column_file), environment)

        # Find unknown tags
        table_tags = tagsync.tags_from_src(src_data_table)
        column_tags = tagsync.tags_from_src(src_data_column)
        atlas_tags = sync_client.tags_from_atlas()
        diff_tags = (table_tags | column_tags)-atlas_tags
        if len(diff_tags) != 0:
            print("Tag(s) missing in Atlas: " + ", ".join(diff_tags).decode("utf-8"))

        schemas = tagsync.schemas_from_src(src_data_table)
        full_tables_atlas = sync_client.get_tables_for_schema_from_atlas(schemas)
        tables = tagsync.tables_from_src(src_data_column)
        full_columns_atlas = sync_client.get_columns_for_tables_from_atlas(tables)

        # Tables only in Atlas
        tables_atlas = set(full_tables_atlas.keys())
        tables_src = set(tagsync.tables_from_src(src_data_table))
        tables_only_atlas = tables_atlas-tables_src
        if len(tables_only_atlas) != 0:
            print("Tables only found in Atlas schema: %s" % (", ".join(tables_only_atlas).decode("utf-8")))
        tables_only_src = tables_src-tables_atlas
        # Tables only in Metadata
        if len(tables_only_src) != 0:
            print("Tables only found in metadata schema: %s" % (", ".join(tables_only_src).decode("utf-8")))

        # Columns only in Metadata
        # Note: columns only in Atlas can right know not be done since we not know all columns in source.
        columns_atlas = set(full_columns_atlas.keys())
        columns_src = set(tagsync.columns_from_src(src_data_column))
        column_only_src = columns_src-columns_atlas
        if len(column_only_src) != 0:
            print("Columns only found in metadata: %s" % (", ".join(column_only_src).decode("utf-8")))

        # Tag diffs on tables
        diffs = tagsync.diff_table_tags(src_data_table, full_tables_atlas)
        for d in diffs:
            (only_src, only_atlas) = diffs[d]
            if len(only_src) != 0:
                print("Atlas missing following tags for table: %s tags: %s" % (d, ", ".join(only_src).decode("utf-8")))
            if len(only_atlas) != 0:
                print("Metadata missing following tags for table: %s tags: %s" % (d, ", ".join(only_atlas).decode("utf-8")))

        # Tag diffs on columns
        diffs = tagsync.diff_column_tags(src_data_column, full_columns_atlas)
        for d in diffs:
            (only_src, only_atlas) = diffs[d]
            if len(only_src) != 0:
                print("Atlas missing following tags for column: %s tags: %s" % (d, ", ".join(only_src).decode("utf-8")))
            if len(only_atlas) != 0:
                print("Metadata missing following tags for column: %s tags: %s" % (d, ", ".join(only_atlas).decode("utf-8")))

    except IOError as e:
        raise ClickException(e.message)


@cli.command("audit_tags", help="A dry run providing audit information about tags. \
It includes differences between source files and Atlas.")
@click.option('-s', '--srcdir', help='The schema for the generated table', default='src/main/tags')
@click.option('-e', '--environment', help='Destination environment', required=True)
@click.option('-c', '--config', help='Config file', type=click.Path(exists=True))
@click.option('--tabletagfile', help='The source file for table tags file', default='table_tags.csv')
@click.option('--columntagfile', help='The source file for column tags file', default='column_tags.csv')
def audit(srcdir, environment, config, tabletagfile, columntagfile):
    _audit(srcdir, environment, config, tabletagfile, columntagfile)


@cli.command("policy_cache_sync", help="Reads a policy cache file copied from hive sercer and"
                                       "set all tags in it in Atlas. Useful when you tagstore"
                                       "in Ranger is out of sync with Atlas.")
@click.option('-e', '--environment',
              help='Destination environment. Used to get Atlas and ranger config.', required=True)
@click.option('-c', '--config', help='Config file', type=click.Path(exists=True))
@click.option('--policycachefile', help='Policy cache file for tags copied from Hives policy cache.', required=True)
@click.option('--tabletagfile', help='Destination file for table tags')
@click.option('--columntagfile', help='Destination file for column tags')
@click.option('--hdfs/--no-hdfs',
              help='Set tags on hive tables corresponding hdfs directory. No effect if tagfiles are created',
              default=False)
def policy_cache_sync(environment, config, policycachefile, tabletagfile, columntagfile, hdfs):
    policycache.extract_policy_cache(
        JSONPropertiesFile(config).get(environment), policycache, tabletagfile, columntagfile, hdfs)


if __name__ == '__main__':
    cli()
