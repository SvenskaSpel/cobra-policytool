# Config file

Before using cobra-policytool for first time you need to configure it for your
environment. It is assumed that you authenticate to all servers using Kerberos.

## Location of config file
You can either provide the config file with the argument `-c` which is the 
recommended way. Otherwise cobra-policytool will look in a couple of locations.
On Mac and Unix it first look in the home directory for 
`~/.config/cobra-policytool/config.json` and second in 
`/etc/cobra-policytool/config.json`. In windows it looks for 
`cobra-policytool\config.json` in the users home directory.


## Content of config file
The config file is a json file containing a property environments, which is an array.
Each object in the array represent an environment and selected using the command line 
option `--environment`, see also [conventions](Conventions.md). Example:
```
{"environments": [ 
  {
    "name": "prod",
    "atlas_api_url": "http://atlas.prod.myorg.com:21000/api/atlas",
    "ranger_api_url": "http://ranger.prod.myorg.com:6080",
    "hive_server": "hiveserver2.prod.myorg.com",
    "hive_port": "10000"
  },{
    "name": "test",
    "atlas_api_url": "http://atlas.test.myorg.com:21000/api/atlas",
    "ranger_api_url": "http://ranger.test.myorg.com:6080",
    "hive_server": "hiveserver2.test.myorg.com",
    "hive_port": "10000"
  }]
}
```
The two environments in the config file above shows the minimum config you need
for each environment. 

In ranger_policies.json you can refer to variables. These can be defined in a variables section
for each environment. This makes our policy definitions very powerful and easy to use the same
file for different setups. In the following example we have defined three environment "prod", "autotest" and 
"misctest". Our environment "autotest" and "misctest" share the same hadoop cluster, but prod has its own. 

```
{ "environments": [{
    "name": "prod",
    "atlas_api_url": "http://atlas.prod.host:21000/api/atlas",
    "ranger_api_url": "http://ranger.prod.host:6080",
    "hive_server": "hiveserver2.prod.myorg.com",
    "hive_port": "10000"
    "variables": [
        { "name": "installation",
          "value": "prod"}
    ],
  },{
    "name": "autotest",
    "atlas_api_url": "http://atlas.test.host:21000/api/atlas",
    "ranger_api_url": "http://ranger.test.host:6080",
    "hive_server": "hiveserver2.test.myorg.com",
    "hive_port": "10000"
    "variables": [
      { "name": "installation",
        "value": "test"}
    ],
  },{
    "name": "misctest",
    "atlas_api_url": "http://atlas.test.host:21000/api/atlas",
    "ranger_api_url": "http://ranger.test.host:6080",
    "hive_server": "hiveserver2.test.myorg.com",
    "hive_port": "10000"
    "variables": [
      { "name": "installation",
        "value": "test"}
    ]
  }]

}
```

Introducing the variable `installation` gives us the possibility to have one variable that defines meaning
per cluster. This is useful for instance for services in the policy file, both "autotest" and "misctest" will 
have the same services. If we prefix or suffix our service name with prod and test respectively we can use
the installation variable in our policy file. You can see how this is done in our 
[example file](../example/ranger_policies.json).