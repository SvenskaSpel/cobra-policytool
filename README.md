# Cobra-policytool

Cobra-policytool is a tool to ease management of
[Apache Ranger](https://ranger.apache.org/) together with tags
in [Apache Atlas](https://atlas.apache.org/). These tools
manage access policies in Hadoop environments. Cobra-policytool makes it easy
to apply configuration files direct to Atlas and Ranger at scale.

The advantages are:
 * configurations can be version controlled
 * changes can be reviewed, audited and tracked
 * integrates with existing CI/CD

Cobra-policytool does also add functionality to Atlas and Ranger.
Cobra-policytool can manage have row level filtering policies for
[Apache Hive](https://hive.apache.org/) based on tags. Ranger requires one
row level policy per table, but with cobra-policytool one can
have one rule per tag. This rule is then expanded by the cobra-policytool
to one rule fore each table having the tag. 

Most often one want to have the same access rights hive tables and corresponding
files and directories on hdfs. Cobra-policytool can automatically convert a policy
for a Hive table to policy for hdfs.

This eases the maintenance and reduce risks for errors.


To be able to use the tool you need to have the right permissions in the
environment you are using. For Atlas you must be able to read and create
tags and to be able to add and delete them from resources. For the Ranger
rules you must be admin, unfortunately.

Cobra-policytool is idempotent, that means you can rerun it as much as
you want, the result will not change if on have not changed the input.

There is an introduction how to use cobra-policytool tool on 
[Medium](https://medium.com/@mrunesson/managing-data-access-policies-in-hive-bba60943b7b4)

A presentation about how Cobra-policytool is used within Svenska Spel can
be found at
[Slideshare](https://www.slideshare.net/MagnusRunesson/practical-experiences-using-atlas-and-ranger-to-implement-gdpr-dataworkssummit-2018).
and [Youtube](https://www.youtube.com/watch?v=MlDQqj5aYOg)

### Goals

* Make it easy to manage access policies and metadata within
a Apache Hadoop environment that uses Apache Atlas and Apache Ranger.
* Provide an easy way to apply policies from configuration files, that can
be version controlled.
* Configuration files shall be easy to generate, for instance from a central
metadata management system.


### Non-Goals

* Handle the security within the Hadoop environment. We rely on
Apache Atlas, Atlas Ranger and other tools within the Hadoop ecosystem.


## Contributing

We welcome contributions. In order for us to be able to accept them,
please review our contributor [guidelines](CONTRIBUTING.md).


## License

This project is released as open source according to the terms laid
out in the [LICENSE](LICENSE.txt).


## Supported features

### Tagging of resources
* Sync of table and column tags from metadata files to Atlas.
* Keep tags between hive corresponding directory on hdfs in sync (use option --hdfs)
* Audit to show differences between metadata and Atlas.
* New tag definitions are automatically added to Atlas on sync.
* Verbose output to provide changes done.
* Authentication using kerberos ticket.

### Creating policies
* Sync policies from metadata file to Ranger.
* Expand tag based row filtering rules to Hive row based filtering.

## Requirements
* Atlas, Ranger, and Hive installed and working.
* Kerberos turned on on the Hadoop cluster, including Atlas and Ranger. Your 
client do also need to have a valid kerberos ticket.
* Python 2.7.
* We have successfully used it on MS Windows, MacOS and Linux.

## Installation

`pip install cobra-policytool`

## Usage of CLI

To get up to date help how to use the tool:
`cobra-policy --help`

For any use where policytool talks to the Atlas server a kerberos ticket must
be available.

Create a configfile matching your environment, see [docs/Configfile.md](docs/Configfile.md). 

Read about the indata files in [docs/indata.md](docs/indata.md).

### Sync tag metadata information to Atlas

Policytool takes files in `--srcdir` directory created according
to [indata specification](docs/indata.md) and sync them with the metadata
store in Hadoop called Atlas. To do this run:
```
$ cobra-policy tags_to_atlas --srcdir src/main/tags/ --environment utv
```
There is an option `--verbose` to get more output from cobra-policytool describing what
tables and columns was changed. Note! If you run same cobra-policytool command twice
you will not get any changes the second time since all changes happened the
first round.

Sync Ranger policies works in a similar fashion, though it requires that
project-name is provided. Project-name is a name of the project
you are working in. It is used to find already existing policies in Ranger and
to be able to separate the ranger rules into multiple projects.
```
$ cobra-policy rules_to_ranger --srcdir src/main/tags/ --environment dev --project-name dimension_out
```

## Usage of API

The package can also be used as a python library. Here is a short example to
use the Atlas Client class.
```
from requests_kerberos import HTTPKerberosAuth
import policytool.atlas

c = policytool.atlas.Client(
        'http://atlas.test.my.org:21000/api/atlas',
         auth=HTTPKerberosAuth())
c.known_tags()
c.get_tables("hadoop_out_utv")
```

For details read the Python doc for the code and look how the command line
client is implemented.

## Other documentation
Beside this document there are more in the [docs directory](docs/). You can
also find a [todo list](TODO.md) including future plans.

We recommend to read the [convention document](docs/Conventions.md) and
[indata document](docs/indata.md) before you start.


---
Copyright 2018 AB SvenskaSpel

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
