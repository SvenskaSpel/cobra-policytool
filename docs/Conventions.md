# Conventions

Within Policytool there are a couple of conventions implemented.

## Hive

Our setup is fully Hive focused. We assume that we have no processing
outside hive. Hence, we have implemented policytool to support
Hive as a first hand citizens. 


## Environment

We have found it useful to have several environments within one
cluster. Environments can be dev, test and prod for development,
testing and production respectively. This is controlled
by suffix all databases/schemas with underscore environment.
For instance if we have the database data_lake. In our Hadoop and
Hive setup we have three databases data_lake_dev, data_lake_test
and data_lake_prod.

## Project name

We have divided our data warehouse into projects. Each with its own
name. One project owns a set of tables in one or several databases.
In Policytool we use project-name to name our Ranger rules. This
way we avoid conflicting naming of rules from different projects.  