# cdc-sink

This tool lets one CockroachDB cluster take a single CDC feed from another
cluster. This feed can send one or more tables.

***Note, only single table works at this time***

For more information on CDC, please see: <https://www.cockroachlabs.com/docs/dev/change-data-capture.html>

## Instructions

_Note that this is subject to change as this is under heavy development right
now._

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
5. Once it starts up, enable a cdc feed from the source cluster
    * `CREATE CHANGEFEED FOR TABLE [source_table] INTO 'experimental-[cdc-sink-url:port]/test.sql' WITH updated,resolved`
    * Be sure to always use the options `updated` and `resolved` as these are
    required for this to work

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
users.

      ```json
      [{"endpoint":"cdc.sql", "source_table":"users", "destination_database":"defaultdb", "destination_table":"users"}]
      ```

      * And the changefeed would be called on the source database

      ```sql
      CREATE CHANGEFEED FOR TABLE users INTO 'experimental-[cdc-sink-url:port]/cdc.sql' WITH updated,resolved
      ```

    * Two table changefeed.
      * Two tables this time, users and customers to two different databases.

      ```json
      [
       {"endpoint":"cdc.sql", "source_table":"users", "destination_database":"global", "destination_table":"users"},
       {"endpoint":"cdc.sql", "source_table":"customers", "destination_database":"success", "destination_table":"customers"},
      ]
      ```

      * And the changefeed would be called on the source database

      ```sql
      CREATE CHANGEFEED FOR TABLE users,customers INTO 'experimental-[cdc-sink-url:port]/cdc.sql' WITH updated,resolved
      ```

  * And don't forget to escape the quotation marks in the prompt:

    ```bash
    ./cdc-sink --config="[{\"endpoint\":\"test.sql\", \"source_table\":\"in_test1\", \"destination_database\":\"defaultdb\", \"destination_table\":\"out_test1\"},{\"endpoint\":\"test.sql\", \"source_table\":\"in_test2\", \"destination_database\":\"defaultdb\", \"destination_table\":\"out_test2\"}]"
    ```

## Limitations (for now)

* only one change feed per cdc-sink instance
* data-types that don't work.
  * bytes

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
