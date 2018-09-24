import copy

"""
Misc utility function to handle a structure(json converted to dict) representing a Ranger policy.
"""


def validate_policy(policy):
    """
    Simple validation a policy include som necessary elements.
    """
    if not policy.has_key("name"):
        raise AttributeError("Policy missing attribute name.")
    if not policy.has_key("policyType"):
        raise AttributeError("Policy {} do not have PolicyType.".format(policy["name"]))
    if not policy.has_key("resources"):
        raise AttributeError("Policy {} do not have resources.".format(policy["name"]))
    if not policy.has_key("policyItems"):
        raise AttributeError("Policy {} do not have policyItems.".format(policy["name"]))


def get_resource_type(policy):
    """
    Return resource type of the policy.
    :param policy: Policy structure.
    :return: Resource type used in policy (database, tag, path).
    """
    if policy["policyType"] != 0:
        raise AttributeError(
            "PolicyType must be 0 to support option expandHiveResourceToHdfs. Policy: {}".format(policy["name"]))
    resource = policy["resources"]
    if resource.has_key("database"):
        return "database"
    if resource.has_key("tag"):
        return "tag"
    if resource.has_key("path"):
        return "path"
    return "unknown"


def extend_tag_policy_with_hdfs(policy):
    """
    Converts incoming policy from only match rules for hive to also
    support hdfs.
    :param policy: Policy with tag resource for hive.
    :return: Policy with access rule for both hive and hdfs.
    """
    if get_resource_type(policy) != "tag":
        raise AttributeError("Policy does not have resource type tag. Policy: {}".format(policy["name"]))
    policy_template_tag = copy.deepcopy(policy)
    policy_template_tag["policyItems"] = []
    for policy_item in policy["policyItems"]:
        policy_item_copy = copy.deepcopy(policy_item)
        policy_item_copy["accesses"] = _expand_hive_tag_accesses_to_file_accesses(policy_item_copy["accesses"])
        policy_template_tag["policyItems"].append(policy_item_copy)
    return policy_template_tag


def _expand_hive_tag_accesses_to_file_accesses(hive_tag_accesses):
    read_access_tag = False
    write_access_tag = False
    for elem in hive_tag_accesses:
        if elem["isAllowed"] and elem['type'] in ["hive:select", "hive:read"]:
            read_access_tag = True
        if elem["isAllowed"] and elem['type'] in ["hive:update", "hive:insert", "hive:create", "hive:drop", "hive:alter", "hive:write"]:
            write_access_tag = True
    result = copy.deepcopy(hive_tag_accesses)
    if read_access_tag:
        result.extend([{
            "type": "hdfs:read",
            "isAllowed": True
        },{
            "type": "hdfs:execute",
            "isAllowed": True
        }])
    if write_access_tag:
        result.extend([{
            "type": "hdfs:write",
            "isAllowed": True
        }])
    return result