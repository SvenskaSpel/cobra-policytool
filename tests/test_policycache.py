import unittest

from policytool import policycache
from policytool.policycache import PolicyCache


class TestPolicyCacheClassMethods(unittest.TestCase):

    def test__extract_resources_for_one_table_object(self):
        indata = {'serviceResources': [
            {u'id': 109410,
             u'resourceElements': {u'table':
                                       {u'isExcludes': False, u'values': [u'table_name'], u'isRecursive': False},
                                   u'database':
                                       {u'isExcludes': False, u'values': [u'db_name'], u'isRecursive': False}}
             }]}
        result = PolicyCache._extract_resources(indata, 'table')
        self.assertEqual(result, {('db_name', 'table_name'): 109410})

    def test__extract_resources_for_no_table_object(self):
        indata = {'serviceResources': [
            {u'id': 109410,
             u'resourceElements': {u'database':
                                       {u'isExcludes': False, u'values': [u'db_name'], u'isRecursive': False}}
             }]}
        result = PolicyCache._extract_resources(indata, 'table')
        self.assertEqual(result, {})


class TestPolicyCache(unittest.TestCase):

    def test__tags_for_resource(self):
        indata = {'serviceResources': [
            {u'isEnabled': True,
             u'id': 109410,
             u'resourceElements': {u'database':
                                       {u'isExcludes': False, u'values': [u'db_name'], u'isRecursive': False}}
             }],
            'tags': {
                "81921": {
                    "type": "mytag"
                },
                "42": {
                    "type": "life"
                }
            },
            "resourceToTagIds": {
                "109410": [
                    81921,
                    42
                ]
            }
        }
        policy_cache = PolicyCache(indata)
        tags = policy_cache._tags_for_resource("109410")
        self.assertEqual(tags, ['mytag', 'life'])
