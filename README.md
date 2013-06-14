HiveJdbcStorageHandler
======================

This project is still work in progress intending to submit a patch for [HIVE-1555](https://issues.apache.org/jira/browse/HIVE-1555).

BASIC USAGE
-----------

hive --auxpath /home/myui/tmp/jdbc-storagehandler.jar;

```sql
CREATE EXTERNAL TABLE pg_model_sample90p (
  feature INT, 
  weight DOUBLE
)
STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler'
TBLPROPERTIES (
  "mapred.jdbc.driver.class"="org.postgresql.Driver",
  "mapred.jdbc.url"="jdbc:postgresql://host01/kddtrack2",
  "mapred.jdbc.username"="myui",
  "mapred.jdbc.input.table.name"="model_sample90p"
);

select * from pg_model_sample90p limit 10;
```
