import unittest
from policytool import rangersync
from mock import MagicMock
import mock


class TestSync(unittest.TestCase):

    def test__convert_hive_accesses_to_file_for_read_resource_rules(self):
        indata = [{
            "type": "select",
            "isAllowed": True
        }, {
            "type": "read",
            "isAllowed": True
        }]
        expected_result = [{
            "type": "read",
            "isAllowed": True
        }, {
            "type": "execute",
            "isAllowed": True
        }]
        result = rangersync._convert_hive_resource_accesses_to_path_resource_accesses(indata)
        self.assertEqual(expected_result, result)

    def test__convert_hive_accesses_to_file_for_write_resource_rules(self):
        indata = [{
            "type": "insert",
            "isAllowed": True
        }]
        expected_result = [{
            "type": "write",
            "isAllowed": True
        }, {
            "type": "read",
            "isAllowed": True
        }, {
            "type": "execute",
            "isAllowed": True
        }]
        result = rangersync._convert_hive_resource_accesses_to_path_resource_accesses(indata)
        self.assertEqual(expected_result, result)

    def test__convert_hive_accesses_to_file_no_access(self):
        indata = [{
            "type": "mupp",
            "isAllowed": True
        }, {
            "type": "hive:write",
            "isAllowed": False
        }]
        expected_result = []
        result = rangersync._convert_hive_resource_accesses_to_path_resource_accesses(indata)
        self.assertEqual(expected_result, result)

    def test__convert_hive_access_rule_no_hdfs_service(self):
        with self.assertRaises(rangersync.RangerSyncError) as e:
            rangersync._convert_hive_resource_policy_to_hdfs_policy({}, {}, {})
        self.assertEqual(
            "Option hdfsService must be set if expandHiveResourceToHdfs is true on a policy with database resource.",
            e.exception.message)

    def test__convert_hive_access_rule_with_wrong_policy_type(self):
        with self.assertRaises(rangersync.RangerSyncError) as e:
            rangersync._convert_hive_resource_policy_to_hdfs_policy({"policyType": 1}, {}, {"hdfsService": "service_hdfs"})
        self.assertEqual(
            "Hive server must be configured when using expandHiveResourceToHdfs option.",
            e.exception.message)

    def test__convert_hive_access_rule_without_hive_client_in_context(self):
        with self.assertRaises(rangersync.RangerSyncError) as e:
            rangersync._convert_hive_resource_policy_to_hdfs_policy({"policyType": 0}, {}, {"hdfsService": "service_hdfs"})
        self.assertEqual("Hive server must be configured when using expandHiveResourceToHdfs option.", e.exception.message)

    def test__convert_hive_access_rule_to_hdfs(self):
        hive_client = type('hive_client', (), {})()
        hive_client.get_location = MagicMock(return_value="hdfs://system/my/path.db")
        resources = {
            "database": {
                "values": ["mydb"],
                "isExcludes": False,
                "isRecursive": False
            },
            "column": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False
            },
            "table": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False
            }
        }
        result = rangersync._get_paths_for_database_resources(hive_client, resources)
        self.assertEqual(["/my/path.db"], result)
        hive_client.get_location.assert_called_once_with("mydb", "*")

    def test__convert_hive_access_rule_to_hdfs_multible_datbases_and_tables(self):
        hive_client = type('hive_client', (), {})()
        hive_client.get_location = MagicMock(side_effect=lambda db, t: "hdfs://system/{}/{}.db".format(db, t))
        resources = {
            "database": {
                "values": ["mydb_1", "mydb_2"],
                "isExcludes": False,
                "isRecursive": False
            },
            "table": {
                "values": ["table_1", "table_2"],
                "isExcludes": False,
                "isRecursive": False
            },
            "column": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False
            }
        }
        result = rangersync._get_paths_for_database_resources(hive_client, resources)
        self.assertEqual(['/mydb_1/table_1.db',
                          '/mydb_1/table_2.db',
                          '/mydb_2/table_1.db',
                          '/mydb_2/table_2.db'],
                         result)
        hive_client.get_location.assert_has_calls([mock.call("mydb_1", "table_1"),
                                                   mock.call("mydb_1", "table_2"),
                                                   mock.call("mydb_2", "table_1"),
                                                   mock.call("mydb_2", "table_2")],
                                                  any_order=True)

    def test__convert_hive_access_rule_to_hdfs_faulty_resource(self):
        hive_client = type('hive_client', (), {})()
        resources = {
            "database": {
                "isExcludes": False,
                "isRecursive": False
            }
        }
        with self.assertRaises(rangersync.RangerSyncError):
            rangersync._get_paths_for_database_resources(hive_client, resources)

    def test__convert_hive_access_rule_for_database_to_hdfs(self):
        policy_template_input = {
            "service": "service_hive",
            "name": "test_policy_rule",
            "policyType": 0,
            "description": "Test rule",
            "resources": {
                "database": {
                    "values": ["my_database"],
                    "isExcludes": False,
                    "isRecursive": False
                },
                "column": {
                    "values": ["*"],
                    "isExcludes": False,
                    "isRecursive": False
                },
                "table": {
                    "values": ["*"],
                    "isExcludes": False,
                    "isRecursive": False
                }
            },
            "policyItems": [{
                "accesses": [{
                    "type": "select",
                    "isAllowed": True
                }, {
                    "type": "read",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }
        policy_template_expected = {
            "service": "service_hdfs",
            "name": "path_test_policy_rule",
            "policyType": 0,
            "description": "Implementing hdfs access for test_policy_rule. Expanded by cobra-policytool.",
            "resources":
                {"path": {
                    "values": ["/apps/hive/warehouse/my_database.db"],
                    "isRecursive": True,
                    "isExcludes": False
                }},
            "policyItems":[{
                "accesses":[{
                    "type":"read",
                    "isAllowed": True
                }, {
                    "type":"execute",
                    "isAllowed": True
                }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }

        hive_client = type('hive_client', (), {})()
        hive_client.get_location = MagicMock(return_value="hdfs://system/apps/hive/warehouse/my_database.db")
        context = {'hive_client': hive_client}
        result = rangersync._convert_hive_resource_policy_to_hdfs_policy(policy_template_input, context, {"hdfsService": "service_hdfs"})
        self.assertEqual(policy_template_expected, result)

    def test__convert_tag_access_rule_to_both_hive_and_hdfs(self):
        policy_template_input = {
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
        policy_template_expected = {
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
                    },{
                        "type": "hdfs:read",
                        "isAllowed": True
                    }, {
                        "type": "hdfs:execute",
                        "isAllowed": True
                    }],
                "users": ["myuser"],
                "delegateAdmin": False
            }]
        }

        result = rangersync. extend_tag_policy_with_hdfs(policy_template_input)
        self.assertEqual(policy_template_expected, result)


