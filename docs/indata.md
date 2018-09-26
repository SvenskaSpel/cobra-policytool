# Indata description
The srcdir (`-s` or `--srcdir` option) is expected to include three files:
* `table_tags.csv`
* `column_tags.csv`
* `ranger_policies.json`

The csv files separated with semicolon(;). The first one include tags on
table level, second is on column level. The json file includes the Ranger 
policies. Example of all files can be found in the [example directory](../example).

## table_tags.csv

[table_tags.csv](../example/table_tags.csv) has the fields:
* schema - schema of the table to process. The schema shall not include 
environment, e.g. it must be hadoop_out and not hadoop_out_prod
* table - table to process
* tags - tags to be set on the table. If several tags are provided they shall
be separated with comma(,). No spaces allowed.

It is recommended to include all your tables, even if they do not have tags.
This way it is easy to see if any important table is missed. Tags removed from 
tables will also be removed in Atlas. Not listed tables will not be touched by
the tool.

## column_tags.csv

[column_tags.csv](../example/column_tags.csv) has the fields:
* schema - schema of the column to process. The schema shall not include 
environment, e.g. it must be hadoop_out and not hadoop_out_prod
* table - table of the column to process
* attribute - Name of the column
* tags - tags to be set on the table. If several tags are provided they shall
be separated with comma(,). No spaces allowed.

It is recommended to include all your columns, even if they do not have tags.
This way it is easy to see if any columns are missed. Tags removed from 
columns will also be removed in Atlas. Not listed columns will not be touched
by the tool.

## ranger_policies.json

Unfortunately, policies are more complex to handle than tags. Hence they are stored as 
json. It is the policy file that gives a meaning to our tags. Though, it can also
handle policies not controlled by tags.

As you can see in the [example file](../example/ranger_policies.json) the top 
level is an array of json objects:
```
[
{
  "command": "apply_tag_row_rule",
  "filters": [...],
  "policy": {...}
},
...
]
```

First field is the `command`. There are two commands `apply_rule` and 
`apply_tag_row_rule`. The `apply_rule` command applies normal Ranger rules.
These have only a second field `policy` on the top level. The 
`apply_tag_row_rule` command takes a number of tags and filters. Each table
with the tag will get a row based policy according to the filter. The 
`apply_tag_row_rule` have two fields beyond `command`; `filters` and `policy`. 

The ranger policy file do also include some variables expanded by policytool.
They are written like `${table}`. Variables provided by cobra-policytool are:
* `project_name` - project-name as provided on the command line.
* `environment`  - environment as provided on the command line.
* `table` - Expand to current table name. Only valid for apply_tar_row_rule filters.
* `schema` - Expand to current table name. Only valid for apply_tar_row_rule filters.
* `end_data_column` - Only valid in apply_tag_row_rule. See its section for details. 

You can also define your own variables in the [configfile](Configfile.md) and refer them
in your policy file.

The variables can be referred anywhere in the `ranger_policies.json` by 
`${variable}`. You can find examples of usage in the 
[example file](../example/ranger_policies.json) Policytool expand the variables
referred in the policy file before sent to Ranger.

Policy tool uses the policy name to find older versions of the policy in Ranger.
If you change the name you may delete the old policies by hand in the Ranger
user interface.

### Command apply_rule

Command `apply_rule` can be used in different ways depending on the policytype.
Currently are the following policy types supported:
* Access (0)
* Masking (1)
* Row filtering (2)

The number in parenthesis is what is used for the field `policyType` to control
the type of the policy.

The following code snippet shows the common fields for all policy types. If you
are familiar with the Ranger API you may recognise the fields. This is what is
sent to Ranger, after the variables are expanded. This means that combination
not described in this document can still be valid and accepted by Ranger. This
way policytool is very generic but also raw since it not do any validation 
itself.

```
{
    "command": "apply_rule",
    "policy": {
        "service": "${installation}_hive",
        "name": "${project_name}_${environment}_vanilla",
        "policyType": 0,
        "description": "Access to data in schemas for vanilla etl",
        "isAuditEnabled": true,
        "resources":( ...)
        "isEnabled": true
}
```

* `service` - The service name for the resource the policy will be applied to.
* `name` - Name of the policy, must be unique. It should either start with
`load_etl_` or `${project_name}_${environment}` 
* `policyType` - type of policy, see above.
* `description` - Free text description.
* `isAuditEnabled` - If audit logging is enabled or not.
* `resources` - Definition of resources impacted by the rule. Note! How these
are described differs depending on service used. See further down for tag 
resources and Hive resources.
* `isEnabled` - If policy is enabled or not.

#### Resource definitions.

Each kind of resource has its own resource definition. For Hive resources we
define database, table and column. The following example match all tables and
columns in our two databases `my_database_1` and `my_database_2`.

```
"resources": {
    "database": {
        "values": ["my_database_1", "my_database_2"],
        "isExcludes": false,
        "isRecursive": false
    },
    "column": {
        "values": ["*"],
        "isExcludes": false,
        "isRecursive": false
    },
    "table": {
        "values": ["*"],
        "isExcludes": false,
        "isRecursive": false
    }
}
```
Note that you can only have one policy per resource type and resource.

Tags are defined in a similar way:
```
"tag": {
    "values": ["PII"],
    "isExcludes": false,
    "isRecursive": false
}
```

Even though we only mention hive and tag resources, one may also use the other
resources supported by Ranger. See the Ranger documentation for details.
 
#### Access policy

Access to resources is controlled by access rules.  It has the policy type number 0.
The following example gives access to my_database for the system user etl and
all members of the group ETL_USERS. Note that we in the `accesses` field cannot
set `isAllowed` to false. So access types where the users shall not have
access must be left out. 

```
{
    "command": "apply_rule",
    "options": {
        "expandHiveResourceToHdfs": true,
        "hdfsService": "svs${installation}_hadoop"
    },
    "policy": {
      "service": "${installation}_hive",
      "name": "${project_name}_${environment}_vanilla",
      "policyType": 0,
      "description": "Access to data in schemas for vanilla etl",
      "isAuditEnabled": true,
      "resources": {
        "database": {
          "values": ["my_database_${environment}"],
          "isExcludes": false,
          "isRecursive": false
        },
        "column": {
          "values": ["*"],
          "isExcludes": false,
          "isRecursive": false
        },
        "table": {
          "values": ["*"],
          "isExcludes": false,
          "isRecursive": false
        }
      },
      "policyItems": [{
        "accesses": [{
          "type": "select",
          "isAllowed": true
        }, {
          "type": "update",
          "isAllowed": true
        }, {
          "type": "create",
          "isAllowed": true
        }, {
          "type": "drop",
          "isAllowed": true
        }, {
          "type": "alter",
          "isAllowed": true
        }, {
          "type": "read",
          "isAllowed": true
        }, {
          "type": "write",
          "isAllowed": true
        }],
        "users": ["etl${user_suffix}"],
        "groups": ["ETL_USERS"],
        "conditions": [],
        "delegateAdmin": false
      }],
      "isEnabled": true
    }
```

What access types exist varies between different resources. For hive the following
access types exists:

* select
* update
* create
* drop
* alter
* read
* write
* index
* lock
* all
* replAdmin

See Ranger and respectively resources documentation for details.

The field `delegateAdmin`, if it is true, gives the user with this policy the
right to delegate its rights to other users

You may have many access rules for one resource but they must all be in the
same policy object and listed as different policy items.

The `options` part in the example tells cobra-policytool to also create a corresponding
rule for hdfs. The option `hdfsService` is used for point out the name of the hdfs service
in Ranger. Note that this can be used both when you point out tables explicitly as in the example
or when using tags.


#### Masking policy

Masking of fields is done with policy type number 1. The policy is has a
sub-object `dataMaskPolicyItems` including information how to do the masking
and what it applies to. Not that masking can currently only be
done in Hive. The following example calls our own anonymize fun function an
all fields tagged PII for the user pii_etl.  
 
```
 {
     "command": "apply_rule",
     "policy": {
       "service": "${installation}_tag",
       "name": "load_etl_mask_pii_data",
       "policyType": 1,
       "description": "Mask all column data tagged PII",
       "isAuditEnabled": true,
       "resources": {
         "tag": {
           "values": ["PII"],
           "isExcludes": false,
           "isRecursive": false
         }
       },
       "dataMaskPolicyItems": [{
           "dataMaskInfo": {
           "dataMaskType": "hive:CUSTOM",
           "valueExpr": "udf.anonymize({col})"
         },
         "accesses": [
           {
             "type": "hive:select",
             "isAllowed": true
           }
         ],
         "users": ["pii_etl${user_suffix}"],
         "groups": [],
         "conditions": [],
         "delegateAdmin": false
       }],
       "isEnabled": true
     }
   }
 ```
Take an extra look on the value of the valueExpr field `udf.anonymize({col})`.
Here you can write any SQL function you want in any combination. When a 
function shall have the value of the field marked with the tag you write 
`{col}`, the same way you do in the Ranger user interface.

### Row filtering policy
Row filtering policies has policy type number 2. It is very similar to a
masking policy but instead of `dataMaskPolicyItems` we have a 
`rowFilterPolicyItems`. In the following example we have a `filterExpr`
that keep customer exist in our whitelist.

```
{
    "command": "apply_rule",
    "policy": {
        "service":"${installation}_hive"
        "policyType": "2",
        "name": "customer_d whitelist filtering",
        "isEnabled": true,
        "isAuditEnabled": true,
        "description": "Filter out customers in whitelist.",
        "resources": {
            "database": {
                "values": ["hadoop_out_utv"],
                "isRecursive": false,
                "isExcludes": false
            },
            "table": {
                "values": ["customer_d"],
                "isRecursive": false,
                "isExcludes": false
            }
        },
        "rowFilterPolicyItems":[{
            "groups": ["CRM_USERS"],
            "users": [],
            "accesses":[{
                "type": "select",
                "isAllowed": true
            }],
            "rowFilterInfo": {
                "filterExpr": "exists (select 1 from whitelist where whitelist.customer_id=customer_d.customer_id)"
            }
        }]
    }
}
```

### Command apply_tag_row_rule
The `apply_rule` command has one to one mapping to the functionality of Ranger.
The same is not true for command `apply_tag_row_rule`. It introduces a more 
general tag usage than Ranger provides. In Ranger you can have one row 
filtering rule per table and the rule is only valid for one table. In a large
environment it is desirable to have one rule for all tables with a specific tag.
This works if naming is very systematic of columns this can be reached. For 
instance, we have a table customer and one order. Both have a field `customer_id`. We want
to only show users within a whitelist. In Ranger we must have one row filtering
per table, one for `customer` and one for `order`. Both tables have the tag
`PII_table`. With `apply_tag_row_rule` we only need one rule.

```
{
  "command": "apply_tag_row_rule",
  "filters": [{
    "groups": [],
    "users": ["pii_etl"],
    "tagFilterExprs": [{
      "tags": ["PII_table"],
      "filterExpr": "exists (select 1 from whitelist where whitelist.customer_id=${table}.customer_id)"
    }]
  }],
  "policy": {
    "service": "${installation}_hive",
    "name": "${project_name}_${environment}_${schema}_${table}",
    "description": "Whitelist rowfiltering policy for Service: ${schema}.${table} in ${environment}.",
    "policyType": 2,
    "isEnabled": true,
    "resources": {
      "database": {
        "isExcludes": false,
        "values": ["${schema}_${environment}"],
        "isRecursive": false
      },
      "table": {
        "isExcludes": false,
        "values": ["${table}"],
        "isRecursive": false
      }
    },
    "isAuditEnabled": true
  }
}
```

In the example above shows the rule described. We have a policy section like
for the row filtering policy described for the `apply_rule` command. We do also
have `filters` section. From the example above the filter shall be self 
explanatory. Though, note the reference to `${table}` in `filterExpr`. You can
also see the same variable within the policy. The tag `PII_table` will expand
to several tables. One policy per table will be created. The filter expression
is inserted into the policy and the `${table}` is expanded to the current table.

If multiple tags are provided in one tag filter expression, tables must have all
tags applied to match. 

Several matching filters to one table means the filter expression is added 
together with and.     

Note that policytool uses the tag definitions from the csv files and not from Atlas.