import unittest

from policytool import policyutil


class TestValidatePolicy(unittest.TestCase):

    def test_validate_policy_with_ok_input(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "policyType": 0,
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            },
            "policyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policyutil.validate_policy(policy)

    def test_validate_policy_with_deny_policy_items(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "policyType": 0,
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            },
            "denyPolicyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policyutil.validate_policy(policy)

    def test_validate_policy_with_missing_name(self):
        policy = {
            "service": "service_tag",
            "policyType": 0,
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            },
            "policyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        with self.assertRaises(AttributeError):
            policyutil.validate_policy(policy)
            self.fail("Validate policy did not raise exception for missing name.")

    def test_validate_policy_with_missing_policytype(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            },
            "policyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }

        with self.assertRaises(AttributeError):
            policyutil.validate_policy(policy)
            self.fail("Validate policy did not raise exception for missing policyType.")

    def test_validate_policy_with_missing_resources(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "policyType": 0,
            "description": "Test rule",
            "policyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        with self.assertRaises(AttributeError):
            policyutil.validate_policy(policy)
            self.fail("Validate policy did not raise exception for missing resources.")

    def test_validate_policy_with_missing_policyitems_and_policytype_0(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "policyType": 0,
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            }
        }
        with self.assertRaises(AttributeError):
            policyutil.validate_policy(policy)
            self.fail("Validate policy did not raise exception for missing policyItems.")

    def test_validate_policy_with_missing_policyitems_and_policytype_not_0(self):
        policy = {
            "service": "service_tag",
            "name": "test_policy_rule",
            "policyType": 1,
            "description": "Test rule",
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            }
        }
        policyutil.validate_policy(policy)


class TestResourceType(unittest.TestCase):

    def test_get_resource_type_for_tag(self):
        policy = {
            "policyType": 0,
            "resources": {
                "tag": {
                    "values": [
                        "visible"
                    ],
                    "isExcludes": False,
                    "isRecursive": True
                }
            }
        }
        self.assertEqual("tag", policyutil.get_resource_type(policy))

    def test_get_resource_type_for_database(self):
        policy = {
            "policyType": 0,
            "resources": {
                "database": {
                    "values": [
                        "my_db"
                    ]
                },
                "column": {
                    "values": [
                        "*"
                    ]
                },
                "table": {
                    "values": [
                        "*"
                    ]
                }
            }
        }
        self.assertEqual("database", policyutil.get_resource_type(policy))

    def test_get_resource_type_for_path(self):
        policy = {
            "policyType": 0,
            "resources": {
                "path": {
                    "values": [
                        "/my/path"
                    ]
                }
            },
        }
        self.assertEqual("path", policyutil.get_resource_type(policy))

    def test_get_resource_type_when_not_found_must_return_unknown(self):
        policy = {
            "policyType": 0,
            "resources": {}
        }
        self.assertEqual("unknown", policyutil.get_resource_type(policy))

    def test_get_resource_type_when_not_providing_policytype_0_must_fail(self):
        policy = {
            "name": "foo",
            "policyType": 1,
            "resources": {
                "path": {
                    "values": [
                        "/my/path"
                    ]
                }
            },
        }
        with self.assertRaises(AttributeError) as e:
            policyutil.get_resource_type(policy)
            self.fail("get_resource_type did not raise exception for missing policyItems.")
        self.assertEqual(
            "PolicyType must be 0 to support option expandHiveResourceToHdfs. Policy: foo",
            e.exception.message)


class TestExtendTagPolicyWithHdfs(unittest.TestCase):

    def test_read_only_access(self):
        policy_input = {
            "policyType": 0,
            "resources": {
                "tag": {}
            },
            "policyItems": [{
                "accesses": [{
                    "type": "hive:select",
                    "isAllowed": True
                }, {
                    "type": "hive:read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policy_expected = {
            "policyItems": [{
                "accesses": [{
                    "isAllowed": True,
                    "type": "hive:select"
                }, {
                    "isAllowed": True,
                    "type": "hive:read"
                }, {
                    "isAllowed": True,
                    "type": "hdfs:read"
                }, {
                    "isAllowed": True,
                    "type": "hdfs:execute"
                }],
                "delegateAdmin": False,
                "users": ["myuser"]
            }],
            "policyType": 0,
            "resources": {
                "tag": {}
            }
        }
        self.assertEqual(policy_expected, policyutil.extend_tag_policy_with_hdfs(policy_input))

    def test_write_access(self):
        policy_input = {
            "policyType": 0,
            "resources": {
                "tag": {}
            },
            "policyItems": [{
                "accesses": [{
                    "type": "hive:insert",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policy_expected = {
            "policyItems": [{
                "accesses": [{
                    "isAllowed": True,
                    "type": "hive:insert"
                }, {
                    "isAllowed": True,
                    "type": "hdfs:write"
                }],
                "delegateAdmin": False,
                "users": ["myuser"]
            }],
            "policyType": 0,
            "resources": {
                "tag": {}
            }
        }
        self.assertEqual(policy_expected, policyutil.extend_tag_policy_with_hdfs(policy_input))

    def test_deny_write_access(self):
        policy_input = {
            "policyType": 0,
            "resources": {
                "tag": {}
            },
            "denyPolicyItems": [{
                "accesses": [{
                    "type": "hive:insert",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policy_expected = {
            "denyPolicyItems": [{
                "accesses": [{
                    "isAllowed": True,
                    "type": "hive:insert"
                }, {
                    "isAllowed": True,
                    "type": "hdfs:write"
                }],
                "delegateAdmin": False,
                "users": ["myuser"]
            }],
            "policyType": 0,
            "resources": {
                "tag": {}
            }
        }
        self.assertEqual(policy_expected, policyutil.extend_tag_policy_with_hdfs(policy_input))
