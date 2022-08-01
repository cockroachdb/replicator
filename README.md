# cdc-sink

This tool lets one CockroachDB cluster ingest CDC feeds from another CockroachDB cluster or ingest
replication feeds from other databases.

For more information on CDC, please
see: <https://www.cockroachlabs.com/docs/stable/stream-data-out-of-cockroachdb-using-changefeeds.html>

***This is just a prototype and is not officially supported by Cockroach Labs.***

We cannot provide support for it at this time but may in the future. Use of this tool is entirely at
your own risk and Cockroach Labs makes no guarantees or warranties about its operation.

## Overview

- A source CRDB cluster emits changes in near-real time via the enterprise `CHANGEFEED` feature.
- `cdc-sink` accepts the changes via the HTTP end point.
- `cdc-sink` applies the changes to the target cluster.

```text
+------------------------+
|     source CRDB        |
|                        |
| CDC {source_table(s)}  |
+------------------------+
          |
          V
   http://ip:26258
+---------------------+
|       cdc-sink      |
|                     |
+---------------------+
          |
          V
 postgresql://ip:26257
+------------------------+
|    target CRDB         |
| {destination_table(s)} |
+------------------------+

```

## Instructions

*Note that this is subject to change as this is still under development.*

1. In the source cluster, choose the table(s) that you would like to replicate.
2. In the destination cluster, re-create those tables within a
   single [SQL database schema](https://www.cockroachlabs.com/docs/stable/sql-name-resolution#naming-hierarchy)
   and match the table definitions exactly:
  - Don't create any new constraints on any destination table(s).
  - Don't have any foreign key relationships with the destination table(s).
  - It's imperative that the columns are named exactly the same in the destination table.
3. Start `cdc-sink` either on
  - all nodes of the destination cluster
  - one or more servers that can reach the destination cluster
4. Set the cluster setting of the source cluster to enable range feeds:
   `SET CLUSTER SETTING kv.rangefeed.enabled = true`
5. Once it starts up, enable a cdc feed from the source cluster
  - `CREATE CHANGEFEED FOR TABLE [source_table]`
    `INTO 'http://[cdc-sink-host:port]/target_database/target_schema' WITH updated,resolved`
  - The `target_database` path element is the name passed to the `CREATE DATABASE` command.
  - The `target_schema` element is the name of
    the [user-defined schema](https://www.cockroachlabs.com/docs/stable/create-schema.html) in the
    target database. By default, every SQL `DATABASE` has a `SCHEMA` whose name is "`public`".
  - Be sure to always use the options `updated` and `resolved` as these are required for this to
    work.
  - The `protect_data_from_gc_on_pause` option is not required, but can be used in situations where
    a `cdc-sink` instance may be unavailable for periods longer than the source's garbage-collection
    window (25 hours by default).

## Theory of operation

- `cdc-sink` receives a stream of partially-ordered row mutations from one of more nodes in the
  source cluster.
- The two leading path segments from the URL,
  `/target_database/target_schema`, are combined with the table name from the mutation payload to
  determine the target table:
  `target_database.target_schema.source_table_name`.
- The incoming mutations are staged on a per-table basis into the target cluster, which is ordered
  by the `updated` MVCC timestamps.
- Upon receipt of a `resolved` timestamp, mutations whose timestamps are less that the new, resolved
  timestamp are dequeued.
- The dequeued mutations are applied to the target tables, either as
  `UPSERT` or `DELETE` operations.
- The `resolved` timestamp is stored for future use.

There should be, at most, one source `CHANGEFEED` per target `SCHEMA`. Multiple instances
of `cdc-sink` should be used in production scenarios.

The behavior described above, staging and application as two phases, ensures that the target tables
will be in a transactionally-consistent state with respect to the source database. This is desirable
in a steady-state configuration, however it imposes limitations on the maximum throughput or
transaction size that `cdc-sink` is able to achieve. In situations where a large volume or a high
rate of data must be transferred, e.g.: an initial CDC `CHANGEFEED` backfill, the changefeed may be
created with the `?immediate=true` query parameter in order to apply all mutations to the target
table without staging them.

`cdc-sink` relies heavily upon the delivery guarantees provided by a CockroachDB `CHANGEFEED`. When
errors are encountered while staging or applying mutations, `cdc-sink` will return them to the
incoming changefeed request. Errors will then become visible in the output of `SHOW JOBS` or in the
administrative UI on the source cluster.

### Table schema

Each staging table has a schema as follows. They are created in a database named `_cdc_sink` by
default.

```sql
CREATE TABLE _targetDB_targetSchema_targetTable
(
    nanos   INT    NOT NULL,
    logical INT    NOT NULL,
    key     STRING NOT NULL,
    mut     JSONB  NOT NULL,
    PRIMARY KEY (nanos, logical, key)
)
```

There is also a timestamp-tracking table.

```sql
CREATE TABLE _timestamps_
(
    key     STRING NOT NULL PRIMARY KEY,
    nanos   INT8   NOT NULL,
    logical INT8   NOT NULL
)
```

## Changefeed replication

```text
Usage:
  cdc-sink start [flags]

Flags:
      --bindAddr string         the network address to bind to (default ":26258")
      --conn string             cockroach connection string (default "postgresql://root@localhost:26257/?sslmode=disable")
      --disableAuthentication   disable authentication of incoming cdc-sink requests; not recommended for production.
  -h, --help                    help for start
      --immediate               apply mutations immediately without preserving transaction boundaries
      --stagingDB string        a sql database name to store metadata information in (default "_cdc_sink")
      --tlsCertificate string   a path to a PEM-encoded TLS certificate chain
      --tlsPrivateKey string    a path to a PEM-encoded TLS private key
      --tlsSelfSigned           if true, generate a self-signed TLS certificate valid for 'localhost'

Global Flags:
      --logDestination string   write logs to a file, instead of stdout
      --logFormat string        choose log output format [ fluent, text ] (default "text")
  -v, --verbose count           increase logging verbosity to debug; repeat for trace
```

### Example

```bash
# install cdc-sink
go install github.com/cockroachdb/cdc-sink@latest

# source CRDB is single node
cockroach start-single-node --listen-addr :30000 --http-addr :30001 --store cockroach-data/30000 --insecure --background

# target CRDB is single node as well
cockroach start-single-node --listen-addr :30002 --http-addr :30003 --store cockroach-data/30002 --insecure --background

# source ycsb.usertable is populated with 10 rows
cockroach workload init ycsb 'postgresql://root@localhost:30000/ycsb?sslmode=disable' --families=false --insert-count=10

# target ycsb.usertable is empty
cockroach workload init ycsb 'postgresql://root@localhost:30002/ycsb?sslmode=disable' --families=false --insert-count=0

# source has rows, target is empty
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"

# create staging database for cdc-sink
cockroach sql --port 30002 --insecure -e "CREATE DATABASE _cdc_sink"
# cdc-sink started as a background task. Remove the tls flag for CockroachDB <= v21.1
cdc-sink start --bindAddr :30004 --tlsSelfSigned --disableAuthentication --conn 'postgresql://root@localhost:30002/?sslmode=disable' &

# start the CDC that will send across the initial data snapshot
# Versions of CRDB before v21.2 should use the experimental-http:// URL scheme
cockroach sql --insecure --port 30000 <<EOF
-- add enterprise license
-- SET CLUSTER SETTING cluster.organization = 'Acme Company';
-- SET CLUSTER SETTING enterprise.license = 'xxxxxxxxxxxx';
SET CLUSTER SETTING kv.rangefeed.enabled = true;
CREATE CHANGEFEED FOR TABLE YCSB.USERTABLE
  INTO 'webhook-https://127.0.0.1:30004/ycsb/public?insecure_tls_skip_verify=true'
  WITH updated, resolved='10s',
       webhook_sink_config='{"Flush":{"Messages":1000,"Frequency":"1s"}}';
EOF

# source has rows, target is the same as the source
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"

# start YCSB workload and the incremental updates happens automagically
cockroach workload run ycsb 'postgresql://root@localhost:30000/ycsb?sslmode=disable' --families=false --insert-count=100 --concurrency=1 &

# source updates are applied to the target
cockroach sql --port 30000 --insecure -e "select * from ycsb.usertable"
cockroach sql --port 30002 --insecure -e "select * from ycsb.usertable"
```

Here's an example Prometheus configuration to scrape `cdc-sink`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: cdc-sink
    metrics_path: /_/varz
    scheme: https
    tls_config:
      insecure_skip_verify: true
    static_configs:
      - targets: [ '127.0.0.1:30004' ]
```

### Backfill mode

The `--backfillWindow` flag can be used with logical-replication modes to ignore source transaction
boundaries if the replication lag exceeds a certain threshold or when first populating data into the
target database. This flag is useful when `cdc-sink` is not expected to be run continuously and will
need to catch up to the current state in the source database.

### Immediate mode

Immediate mode writes incoming mutations to the target schema as soon as they are received, instead
of waiting for a resolved timestamp. Transaction boundaries from the source database will not be
preserved, but overall load on the destination database will be reduced. This sacrifices transaction
atomicity for performance, and may be appropriate in eventually-consistency use cases.

Immediate mode is enabled by passing the `--immediate` flag.

### Data application behaviors

`cdc-sink` supports a number of behaviors that modify how mutations are applied to the target
database. These behaviors can be enabled for both cluster-to-cluster and logical-replication uses
of `cdc-sink`. At present, the configuration is stored in the target CockroachDB cluster in
the `_cdc_sink.apply_config` table. The configuration table is polled once per minute for changes or
in response to a `SIGHUP` sent to the`cdc-sink` process. The active apply configuration is available
from a `/_/config/apply` endpoint.

#### Compare-and-set

A compare-and-set (CAS) mode allows `cdc-sink` to discard some updates, based on version- or
time-like fields.

**The use of CAS mode intentionally discards mutations and may cause data inconsistencies.**

Consider a table with a `version INT` column. If `cdc-sink` receives a message to update some row in

[//]: # (this table, the update will only be applied if there is no pre-existing row in the destination)
table, or if the update's `version` value is strictly greater than the existing row in the
destination table. That is, updates are applied only if they would increase the `version` column or
if they would insert a new row.

To opt into CAS mode for a feed, set the `cas_order` column in the `apply_config` table for one or
more columns in a table. The `cas_order` column must have unique, contiguous values within any
target table. The default value of zero indicates that a column is not subject to CAS behavior.

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, cas_order)
VALUES ('some_db', 'public', 'my_table', 'major_version', 1),
    ('some_db', 'public', 'my_table', 'minor_version', 2),
    ('some_db', 'public', 'my_table', 'patch_version', 3);
```

When multiple CAS columns are present, they are compared as a tuple. That is, the second column is
only compared if the first column value is equal between the source and destination clusters. In
this multi-column case, the following pseudo-sql clause is applied to each incoming update:

```sql
WHERE existing IS NULL OR (
  (incoming.version_major, incoming.version_minor, incoming.version_patch) >
  (existing.version_major, existing.version_minor, existing.version_patch)
)
```

Deletes from the source cluster are always applied.

#### Deadlines

A deadline mode allows `cdc-sink` to discard some updates, by comparing a time-like field to the
destination cluster's time. This is useful when a feed's data is advisory only or changefeed
downtime is expected.

**The use of deadlines intentionally discards mutations and may cause data inconsistencies.**

To opt into deadline mode for a table, set the `deadline` column in the `apply_config` table for one
or more columns in a table. The default value of zero indicates that the column is not subject to
deadline behavior.

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, deadline)
VALUES ('some_db', 'public', 'my_table', 'updated_at', '1m');
```

Given the above configuration, each incoming update would behave as though it were filtered with a
clause such as `WHERE incoming.updated_at > now() - '1m'::INTERVAL`.

Deletes from the source cluster are always applied.

#### Ignore columns

By default, `cdc-sink` will reject incoming mutations that have columns which do not map to a column
in the destination table. This behavior can be selectively disabled by setting the `ignore` column
in the `apply_config` table.

**The use of ignore mode intentionally discards columns and may cause data inconsistencies.**

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, ignore)
VALUES ('some_db', 'public', 'my_table', 'dont_care', true);
```

Ignore mode is mostly useful when using logical replication modes and performing schema changes.

#### Rename columns

By default, `cdc-sink` uses the column names in a target table when decoding incoming payloads. An
alternate column name can be provided by setting the `src_name` column in the `apply_config` table.

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, src_name)
VALUES ('some_db', 'public', 'my_table', 'column_in_target_cluster', 'column_in_source_data');
```

Renaming columns is mostly useful when using logical replication modes and performing schema
changes.

#### Substitute expressions

Substitute expressions allow incoming column data to be transformed with arbitrary SQL expressions
when being applied to the target table.

**The use of substitute expressions may cause data inconsistencies.**

Consider the following, contrived, case of multiplying a value by two before applying it:

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, expr)
VALUES ('some_db', 'public', 'my_table', 'value', '2 * $0');
```

The substitution string `$0` will expand at runtime to the value of the incoming data. Expressions
may repeat the `$0` expression. For example `$0 || $0` would concatenate a value with itself.

In some logical-replication cases, it may be desirable to entirely discard the incoming mutation and
replace the value:

```sql
UPSERT
INTO _cdc_sink.apply_config (target_db, target_schema, target_table, target_column, expr)
VALUES ('some_db', 'public', 'my_table', 'did_migrate', 'true');
```

In cases where a column exists only in the target database, using a `DEFAULT` for the column is
preferable to using a substitute expression.

## PostgreSQL logical replication

An alternate application of `cdc-sink` is to connect to a PostgreSQL-compatible database instance to
consume a logical replication feed. This is primarily intended for migration use-cases, in which it
is desirable to have a minimum- or zero-downtime migration from PostgreSQL to CockroachDB.

```text
Usage:
  cdc-sink pglogical [flags]

Flags:
      --applyTimeout duration     the maximum amount of time to wait for an update to be applied (default 30s)
      --backfillWindow duration   use a high-throughput, but non-transactional mode if replication is this far behind
      --bytesInFlight int         apply backpressure when amount of in-flight mutation data reaches this limit (default 10485760)
      --fanShards int             the number of concurrent connections to use when writing data in fan mode (default 16)
  -h, --help                      help for pglogical
      --immediate                 apply data without waiting for transaction boundaries
      --loopName string           identify the replication loop in metrics (default "pglogical")
      --metricsAddr string        a host:port to serve metrics from at /_/varz
      --publicationName string    the publication within the source database to replicate
      --retryDelay duration       the amount of time to sleep between replication retries (default 10s)
      --slotName string           the replication slot in the source database (default "cdc_sink")
      --sourceConn string         the source database's connection string
      --stagingDB string          a SQL database to store metadata in (default "_cdc_sink")
      --standbyTimeout duration   how often to commit the consistent point (default 5s)
      --targetConn string         the target cluster's connection string
      --targetDB string           the SQL database in the target cluster to update
      --targetDBConns int         the maximum pool size to the target cluster (default 1024)

Global Flags:
      --logDestination string   write logs to a file, instead of stdout
      --logFormat string        choose log output format [ fluent, text ] (default "text")
  -v, --verbose count           increase logging verbosity to debug; repeat for trace
```

The theory of operation is similar to the standard use case, the only difference is that `cdc-sink`
connects to the source database to receive a replication feed, rather than act as the target for a
webhook.

### Postgres Replication Setup

- A review of
  PostgreSQL [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
  will be useful to establish the limitations of logical replication.
- Run `CREATE PUBLICATION my_pub FOR ALL TABLES;` in the source database. The name `my_pub` can be
  changed as desired and must be provided to the `--publicationName` flag. Specifying only a subset
  of tables in the source database is possible.
- Run `SELECT pg_create_logical_replication_slot('cdc_sink', 'pgoutput');` in the source database.
  The value `cdc_sink` may be changed and should be passed to the `--slotName` flag.
- Run `SELECT pg_export_snapshot();` to create a consistent point for bulk data export and leave the
  source PosgreSQL session open. (Snapshot ids are valid only for the lifetime of the session which
  created them.)
  The value returned from this function can be passed
  to [`pg_dump --snapshot <snapshot_id>`](https://www.postgresql.org/docs/current/app-pgdump.html)
  that is subsequently
  [IMPORTed into CockroachDB](https://www.cockroachlabs.com/docs/stable/migrate-from-postgres.html)
  or used with
  [`SET TRANSACTION SNAPSHOT 'snapshot_id'`](https://www.postgresql.org/docs/current/sql-set-transaction.html)
  if migrating data using a SQL client.
- Complete the bulk data migration before continuing.
- Run `CREATE DATABASE IF NOT EXISTS _cdc_sink;` in the target cluster to create a staging arena.
- Run `cdc-sink pglogical` with at least the `--publicationName`, `--sourceConn`, `--targetConn`,
  and `--targetDB` flags after the bulk data migration has been completed. This will catch up with
  all database mutations that have occurred since the replication slot was created.

If you pass `--metricsAddr 127.0.0.1:13013`, a Prometheus-compatible HTTP endpoint will be available
at `/_/varz`. A trivial health-check endpoint is also available at `/_/healthz`.

To clean up from the above:

- `SELECT pg_drop_replication_slot('cdc_sink');`
- `DROP PUBLICATION my_pub;`

## MySQL/MariaDB Replication

Another possibility is to connect to a MySQL/MariaDB database instance to consume a
transaction-based replication feed using global transaction identifiers (GTIDs). This is primarily
intended for migration use-cases, in which it is desirable to have a minimum- or zero-downtime
migration from MySQL to CockroachDB. For an overview of MySQL replication with GTIDs, refer to
[MySQL Replication with Global Transaction Identifiers](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html)
. For an overview of MariaDB replication refer to
[MariaDB Replication Overview](https://mariadb.com/kb/en/replication-overview/).

```text
Usage:
  cdc-sink mylogical [flags]

Flags:
      --applyTimeout duration     the maximum amount of time to wait for an update to be applied (default 30s)
      --backfillWindow duration   use a high-throughput, but non-transactional mode if replication is this far behind
      --bytesInFlight int         apply backpressure when amount of in-flight mutation data reaches this limit (default 10485760)
      --defaultGTIDSet string     default GTIDSet. Used if no state is persisted
      --fanShards int             the number of concurrent connections to use when writing data in fan mode (default 16)
  -h, --help                      help for mylogical
      --immediate                 apply data without waiting for transaction boundaries
      --loopName string           identify the replication loop in metrics (default "mylogical")
      --metricsAddr string        a host:port to serve metrics from at /_/varz
      --retryDelay duration       the amount of time to sleep between replication retries (default 10s)
      --sourceConn string         the source database's connection string
      --stagingDB string          a SQL database to store metadata in (default "_cdc_sink")
      --standbyTimeout duration   how often to commit the consistent point (default 5s)
      --targetConn string         the target cluster's connection string
      --targetDB string           the SQL database in the target cluster to update
      --targetDBConns int         the maximum pool size to the target cluster (default 1024)

Global Flags:
      --logDestination string   write logs to a file, instead of stdout
      --logFormat string        choose log output format [ fluent, text ] (default "text")
  -v, --verbose count           increase logging verbosity to debug; repeat for trace
```

The theory of operation is similar to the standard use case, the only difference is that `cdc-sink`
connects to the source database to receive a replication feed, rather than act as the target for a
webhook.

### MySQL/MariaDB Replication Setup

- The MySQL server should have the following settings:

```text
      --gtid-mode=on
      --enforce-gtid-consistency=on
      --binlog-row-metadata=full
```

- If server is MariaDB, it should have the following settings:

```text
      --log-bin
      --server_id=1
      --log-basename=master1
      --binlog-format=row
      --binlog-row-metadata=full
```

- Verify the master status, on the MySQL/MariaDB server

```text
    show master status;
```

- Perform a backup of the database. Note: Starting with MariaDB 10.0.13, mysqldump automatically
  includes the GTID position as a comment in the backup file if either the --master-data or
  --dump-slave option is used.

```bash
   mysqldump -p db_name > backup-file.sql
```

- Note the GTID state at the beginning of the backup, as reported in the backup file. For instance:

```SQL
-- MySQL:
--
-- GTID state at the beginning of the backup
--

SET
@@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '6fa7e6ef-c49a-11ec-950a-0242ac120002:1-8';
```

```SQL
-- MariaDB:
--
-- GTID to start replication from
--

SET
GLOBAL gtid_slave_pos='0-1-1';
```

- Import the database into Cockroach DB, following the instructions
  at [Migrate from MySQL](https://www.cockroachlabs.com/docs/stable/migrate-from-mysql.html).
- Run `cdc-sink mylogical` with at least the `--sourceConn`, `--targetConn`
  , `--defaultGTIDSet` and `--targetDB`. Set `--defaultGTIDSet` to the GTID state shown above.

## Security Considerations

At a high level, `cdc-sink` accepts network connections to apply arbitrary mutations to the target
cluster. In order to limit the scope of access, [JSON Web Tokens](https://jwt.io) are used to
authorize incoming connections and limit them to writing to a subset of the SQL databases or
user-defined schemas in the target cluster.

A minimally-acceptable JWT claim is shown below. It includes the standard `jti` token identifier
field, and a custom claim for cdc-sink. This custom claim is defined in a globally-unique namespace
and most JWT-enabled authorization providers support adding custom claims. It contains a list of one
or more target databases or user-defined schemas. Wildcards for databases or schema names are
supported.

Acceptable JWT tokens must be signed with RSA or EC keys. HMAC and  `None`
signatures are rejected. The PEM-formatted public signing keys must be added to the
`_cdc_sink.jwt_public_keys` table. If a specific token needs to be revoked, its `jti`
value can be added to the `_cdc_sink.jwt_revoked_ids` table. These tables will be re-read every
minute by the `cdc-sink` process. A `HUP` signal can be sent to force an early refresh.

The encoded token is provided to the `CREATE CHANGEFEED` using
the `WITH webhook_auth_header='Bearer <encoded token>'`
[option](https://www.cockroachlabs.com/docs/stable/create-changefeed.html#options). In order to
support older versions of CockroachDB, the encoded token may also be specified as a query
parameter `?access_token=<encoded token>`. Note that query parameters may be logged by intermediate
loadbalancers, so caution should be taken.

### Token Quickstart

This example uses `OpenSSL`, but the ultimate source of the key materials doesn't matter, as long as
you have PEM-encoded RSA or EC keys.

```bash
# Generate a EC private key using OpenSSL.
openssl ecparam -out ec.key -genkey -name prime256v1

# Write the public key components to a separate file.
openssl ec -in ec.key -pubout -out ec.pub

# Upload the public key for all instances of cdc-sink to find it.
cockroach sql -e "INSERT INTO _cdc_sink.jwt_public_keys (public_key) VALUES ('$(cat ec.pub)')"

# Reload configuration, or wait one minute.
killall -HUP cdc-sink

# Generate a token which can write to the ycsb.public schema.
# The key can be decoded using the debugger at https://jwt.io.
# Add the contents of out.jwt to the CREATE CHANGEFEED command:
# WITH webhook_auth_header='Bearer <out.jws>'
cdc-sink make-jwt -k ec.key -a ycsb.public -o out.jwt
```

### External JWT Providers

The `make-jwt` subcommand also supports a `--claim` option, which will simply print a JWT claim
template, which can be signed by your existing JWT provider. The PEM-formatted public key or keys
for that provider will need to be inserted into tho `_cdc_sink.jwt_public_keys` table, as above.
The `iss` (issuers) and `jti` (token id) fields will likely be specific to your auth provider, but
the custom claim must be retained in its entirety.

`cdc-sink make-jwt -a 'database.schema' --claim`:

```json
{
  "iss": "cdc-sink",
  "jti": "d5ffa211-8d54-424b-819a-bc19af9202a5",
  "https://github.com/cockroachdb/cdc-sink": {
    "schemas": [
      [
        "database",
        "schema"
      ]
    ]
  }
}
```

## Limitations

*Note that while limitation exists, there is no warning or error that is thrown when they are
violated. It may result in missing data or the stream my stop.*

- all the limitations from CDC hold true.
  - See <https://www.cockroachlabs.com/docs/dev/change-data-capture.html#known-limitations>
- schema changes do not work,
  - in order to perform a schema change
    1. stop the change feed
    2. stop cdc-sink
    3. make the schema changes to both tables
    4. start cdc-sink
    5. restart the change feed
- constraints on the destination table
  - foreign keys
    - there is no guarantee that foreign keys between two tables will arrive in the correct order so
      please only use them on the source table
    - different table constraints
      - anything that has a tighter constraint than the original table may break the streaming
- the schema of the destination table must match the primary table exactly

## Expansions

While the schema of the secondary table must match that of the primary table, specifically the
primary index. There are some other changes than can be made on the destination side.

- Different and new secondary indexes are allowed.
- Different zone configs are allowed.
- Adding new computed columns, that cannot reject any row, should work.
