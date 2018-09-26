import copy

"""
Misc utility function to handle a structure(json converted to dict) representing a Ranger policy.
"""


def validate_policy(policy):
    """
    Simple validation a policy include som necessary elements.
    """
    if "name" not in policy:
        raise AttributeError("Policy missing attribute name.")
    if "policyType" not in policy:
        raise AttributeError("Policy {} do not have PolicyType.".format(policy["name"]))
    if policy["policyType"] not in [0, 1, 2]:
        raise AttributeError("Policy {} must have PolicyType 0, 1, or 2.".format(policy["name"]))
    if "resources" not in policy:
        raise AttributeError("Policy {} do not have resources.".format(policy["name"]))
    if policy["policyType"] == 0 and not ("policyItems" in policy or "denyPolicyItems" in policy):
        raise AttributeError("Policy {} do not have policyItems nor denyPolicyItems.".format(policy["name"]))


def get_resource_type(policy):
    """
    Return resource type of the policy.
    :param policy: Policy structure.
    :return: Resource type used in policy (database, tag, path).
    """
    if policy["policyType"] != 0:
        raise AttributeError(
            "PolicyType must be 0 to support option expandHiveResourceToHdfs. Policy: {}".format(policy["name"]))
    resources = policy["resources"]
    if "database" in resources:
        return "database"
    if "tag" in resources:
        return "tag"
    if "path" in resources:
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
    if "policyItems" in policy:
        policy_template_tag["policyItems"] = []
        for policy_item in policy["policyItems"]:
            policy_item_copy = copy.deepcopy(policy_item)
            policy_item_copy["accesses"] = _expand_hive_tag_accesses_to_file_accesses(policy_item_copy["accesses"])
            policy_template_tag["policyItems"].append(policy_item_copy)
    if "denyPolicyItems" in policy:
        policy_template_tag["denyPolicyItems"] = []
        for policy_item in policy["denyPolicyItems"]:
            policy_item_copy = copy.deepcopy(policy_item)
            policy_item_copy["accesses"] = _expand_hive_tag_accesses_to_file_accesses(policy_item_copy["accesses"])
            policy_template_tag["denyPolicyItems"].append(policy_item_copy)
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
