from template import apply_context
from collections import namedtuple
import click
import sys
from collections import defaultdict


def apply_commands(policy_commands, context):
    """
    Expands policies for tags to policies for tables
    :param policy_commands: Policies to expand
    :param context: Context describing tables and tags
    :return: policies for tables
    """
    policy_lists = [apply_command(policy_command, context) for policy_command in policy_commands]
    policies = [policy for policies in policy_lists for policy in policies]
    return policies


def apply_command(policy_command, context):
    """
    Expand one tag policy to policies for tables with that tag
    :param policy_command: Policy to expand
    :param context: Context describing tables and tags
    :return: policies for tables
    """
    command = policy_command['command']
    policy_template = policy_command['policy']
    if command == 'apply_rule':
        return apply_rule_command(policy_template, context)
    elif command == 'apply_tag_row_rule':
        return apply_tag_row_rule_command(policy_command['filters'], policy_template, context)
    else:
        raise RangerSyncError("Unknown command: {}".format(command))


def apply_rule_command(policy_template, context):
    return [apply_context(policy_template, context)]


def _tags_to_columns(columns):
    tag_columns = defaultdict(list)
    for column in columns:
        attribute = column['attribute']
        column_tags = filter(None, column['tags'].split(","))
        for column_tag in column_tags:
            tag_columns[column_tag].append(attribute)
    return tag_columns


def apply_tag_row_rule_command(filters, policy_template, context):
    tables = context['tables']
    policies = []
    for table in tables:
        table_name = "{}.{}".format(table['schema'], table['table'])
        columns = context['table_columns'][table_name]
        tag_columns = _tags_to_columns(columns)
        tags = set(table["tags"].split(","))
        row_filters = []
        for filter_ in filters:
            tag_filter_exprs = [
                tagFilterExpr['filterExpr']
                for tagFilterExpr in filter_['tagFilterExprs']
                if set(tagFilterExpr['tags']) <= tags
            ]
            tag_filter_exprs_str = " and ".join(tag_filter_exprs)
            if len(tag_filter_exprs) > 0:
                row_filters.append({
                  "groups": filter_['groups'],
                  "users": filter_['users'],
                  "conditions": [],
                  "accesses": [{
                    "isAllowed": True,
                    "type": "select"
                  }],
                  "rowFilterInfo": {
                    "filterExpr": tag_filter_exprs_str
                  },
                  "delegateAdmin": False
                })
        if len(row_filters) > 0:
            end_date_columns = tag_columns['end_date']
            env = {
                "schema": table['schema'],
                "table": table['table'],
                "end_date_column": end_date_columns[0] if len(end_date_columns) else "<END_DATE_COLUMN_MISSING>"
            }
            new_context = context.extend(env)
            policy_template_copy = policy_template.copy()
            policy_template_copy['rowFilterPolicyItems'] = row_filters
            policy = apply_context(policy_template_copy, new_context)
            policies.append(policy)
    return policies


RuleIdentifier = namedtuple('RuleIdentifier', 'service,name')


class RangerSync:
    def __init__(self, ranger_client, verbose=0, dryrun=False):
        self.ranger_client = ranger_client
        self.verbose = verbose
        self.dryrun = dryrun

    def sync_policies(self, prefixes, policies):
        service_names = set(policy['service'] for policy in policies)
        wanted_policy_identifiers = set(RuleIdentifier(policy.get("service"), policy.get("name")) for policy in policies)
        current_policies = self._current_policies(prefixes, service_names)
        current_policy_identifiers = set(RuleIdentifier(policy.get("service"), policy.get("name")) for policy in current_policies)
        delete_policies = current_policy_identifiers - wanted_policy_identifiers
        self._delete_policies(delete_policies)
        self._apply_policies(policies)

    def _current_policies(self, prefixes, service_names):
        current_policies = []
        for prefix in prefixes:
            for service_name in service_names:
                # TODO: Filter to verify prefix is prefix not in the middle.
                current_policies.extend(self.ranger_client.get_policies_by_name_part(service_name, prefix))
        return current_policies

    def _delete_policies(self, policies):
        for policy_id in policies:
            if self.verbose  > 0:
                click.secho("Delete {}.{}".format(policy_id.service, policy_id.name), file=sys.stderr, fg='red')
            if not self.dryrun:
                response = self.ranger_client.delete_policy_by_name(policy_id.service, policy_id.name)
                if self.verbose > 0:
                    print(response.status_code)
                    print(response.reason)
                    print(response.text)

    def _apply_policies(self, policies):
        for policy in policies:
            if self.verbose  > 0:
                click.secho("Update {}.{}".format(policy['service'], policy['name']), file=sys.stderr, fg='green')
                print(policy)
            response = self.ranger_client.apply_policy(policy, self.verbose, self.dryrun)
            if self.verbose > 0:
                print(response)


class RangerSyncError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
