# cdc-sink

This tool lets one CockroachDB cluster take CDC feeds from one or more
CockroachDB clusters.

For more information on CDC, please see: <https://www.cockroachlabs.com/docs/dev/change-data-capture.html>

***This is just a prototype and is not officially supported by Cockroach Labs.***

We cannot provide support for it at this time but may in the future.

## Overview

- Source CRDB emits changes in realtime via CRDB CDC feature
- CDC sink accepts the changes via the HTTP end point
- CDC sink applies the changes to the target CRDB

```
+---------------------+                          
|     source CRDB     |                           
|                     |                 
| CDC {source_table}  |  
+---------------------+
          |
          V
   http://ip:26258 
+---------------------+
|       CDC sink      |
|CDC SINK {end_point} |
+---------------------+
          |
          V
 postgresql://ip:26257 
+---------------------+
|    target CRDB      |
| {destination_table} |
+---------------------+

```

## Instructions

_Note that this is subject to change as this is still under development._

1. In the source cluster, choose the table(s) you would like to stream.
2. In the destination cluster, re-create those tables and match the schema
exactly.
    * Don't create any new constraints on any destination table.
    * Don't have any foreign key relationships with the destination table.
    * It's imperative that the columns are named exactly the same in the
    destination table.
3. Startup cdc-sink either on
    * all nodes of the destination cluster
    * one or more servers that can reach the destination cluster
4. startup CDC-SINK on all servers that it's running on using the flags listed
below
5. Set the cluster setting of the source cluster to enable range feeds:
`SET CLUSTER SETTING kv.rangefeed.enabled = true`
6. Once it starts up, enable a cdc feed from the source cluster
    * `CREATE CHANGEFEED FOR TABLE [source_table] INTO 'experimental-[cdc-sink-url:port]/test.sql' WITH updated,resolved`
    * Be sure to always use the options `updated` and `resolved` as these are
    required for this to work

### A short exmaple with YCSB workload

```
# install the cdc-sink
go get github.com/cockroachdb/cdc-sink

# source CRDB is single node 
cockroach start-single-node --listen-addr :30000 --http-addr :30001 --store cockroach-data/30000 --insecure --background

# target CRDB is single node as well -- does not need be
cockroach start-single-node --listen-addr :30002 --http-addr :30003 --store cockroach-data/30002 --insecure --background

# source ycsb.usertable is populated with 10 rows
cockroach workload init ycsb postgresql://root@localhost:30000/ycsb?sslmode=disable --families=false --insert-count=10

# target ycsb.usertable is empty
cockroach workload init ycsb postgresql://root@localhost:30002/ycsb?sslmode=disable --families=false --insert-count=0

# source has rows, target is empty
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"

# cdc-sink started as a background task
cdc-sink --port 30004 --conn postgresql://root@localhost:30002/defaultdb?sslmode=disable --config="[{\"endpoint\":\"crdbusertable\", \"source_table\":\"usertable\", \"destination_database\":\"ycsb\", \"destination_table\":\"usertable\"}]" &

# start the CDC that will send across the initial datashapshot 
cockroach sql --insecure --port 30000 <<EOF
-- add enterprise license
-- SET CLUSTER SETTING cluster.organization = 'Acme Company';
-- SET CLUSTER SETTING enterprise.license = 'xxxxxxxxxxxx';
SET CLUSTER SETTING kv.rangefeed.enabled = true;
CREATE CHANGEFEED FOR TABLE YCSB.USERTABLE INTO 'experimental-http://127.0.0.1:30004/crdbusertable' WITH updated,resolved='10s';
EOF

# source has rows, target is the same as the source
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"

# start YCSB workload and the incremental updates happens automagically
cockroach workload run ycsb postgresql://root@localhost:30000/ycsb?sslmode=disable --families=false --insert-count=100 --max-rate=1 --concurrency=1 &

# source updates are applied to the target
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"

```

## Flags

* **conn**
  * the connection string to the destination database,
  * *default:* postgresql://root@localhost:26257/defaultdb?sslmode=disable
* **port**
  * the port that cdc-sink will listen on
  * *default:* 26258
* **sinkDB**
  * the database to hold all temp sink data in the destination cluster
  * probably best not to change this from the default
  * *default:* _CDC_SINK
* **config**
  * This flag must be set. It requires a single line for each table passed in.
  * The format is the following:

  ```json
  [
    {"endpoint":"", "source_table":"", "destination_database":"", "destination_table":""},
    {"endpoint":"", "source_table":"", "destination_database":"", "destination_table":""},
  ]
  ```

  * Each table being updated requires a single line. Note that source database is
not required.
  * Each changefeed requires the same endpoint and you can have more than one table
in a single changefeed.
  * Note that as of right now, only a single endpoint is supported.
  * Here are two examples:
    * Single table changefeed.
      * Source table and destination table are both called
users:

      ```json
      [{"endpoint":"cdc.sql", "source_table":"users", "destination_database":"defaultdb", "destination_table":"users"}]
      ```

      * The changefeed is initialized on the source database:

      ```sql
      CREATE CHANGEFEED FOR TABLE users INTO 'experimental-http://[cdc-sink-ip:26258]/cdc.sql' WITH updated,resolved
      ```

    * Two table changefeed.
      * Two tables this time, users and customers to two different databases:

      ```json
      [
       {"endpoint":"cdc.sql", "source_table":"users", "destination_database":"global", "destination_table":"users"},
       {"endpoint":"cdc.sql", "source_table":"customers", "destination_database":"success", "destination_table":"customers"},
      ]
      ```

      * The changefeed is initialized on the source database:

      ```sql
      CREATE CHANGEFEED FOR TABLE users,customers INTO 'experimental-[cdc-sink-url:port]/cdc.sql' WITH updated,resolved
      ```

  * And don't forget to escape the quotation marks in the prompt:

    ```bash
    ./cdc-sink --config="[{\"endpoint\":\"test.sql\", \"source_table\":\"in_test1\", \"destination_database\":\"defaultdb\", \"destination_table\":\"out_test1\"},{\"endpoint\":\"test.sql\", \"source_table\":\"in_test2\", \"destination_database\":\"defaultdb\", \"destination_table\":\"out_test2\"}]"
    ```

## Limitations (for now)

* data-types that don't work.
  * bytes
* https is not yet supported, only http

## Limitations

*Note that while these limitation exists, there is no warning or error that is
thrown when they are violated.  It may result in missing data or the stream my
stop.*

* all the limitations from CDC hold true.
  * See <https://www.cockroachlabs.com/docs/dev/change-data-capture.html#known-limitations>
* schema changes do not work,
  * in order to perform a schema change
    1. stop the change feed
    2. stop cdc_sink
    3. make the schema changes to both tables
    4. start cdc_sink
    5. restart the change feed
* constraints on the destination table
  * foreign keys
    * there is no guarantee that foreign keys between two tables will arrive in the correct order
    so please only use them on the source table
  * different table constraints
    * anything that has a tighter constraint than the original table may break the streaming
* the schema of the destination table must match the primary table exactly

## Expansions

While the schema of the secondary table must match that of the primary table, specifically the
primary index.  There are some other changes than can be made on the destination side.

* Different and new secondary indexes are allowed.
* Different zone configs are allowed.
* Adding new computed columns, that cannot reject any row, should work.

## Troubleshootings

