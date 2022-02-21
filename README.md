# cdc-sink

This tool lets one CockroachDB cluster ingest CDC feeds from one or more CockroachDB clusters.

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

_Note that this is subject to change as this is still under development._

1. In the source cluster, choose the table(s) that you would like to replicate.
2. In the destination cluster, re-create those tables within a
   single [SQL database schema](https://www.cockroachlabs.com/docs/stable/sql-name-resolution#naming-hierarchy)
   and match the table definitions exactly:
    * Don't create any new constraints on any destination table(s).
    * Don't have any foreign key relationships with the destination table(s).
    * It's imperative that the columns are named exactly the same in the destination table.
3. Start `cdc-sink` either on
    * all nodes of the destination cluster
    * one or more servers that can reach the destination cluster
4. Set the cluster setting of the source cluster to enable range feeds:
   `SET CLUSTER SETTING kv.rangefeed.enabled = true`
5. Once it starts up, enable a cdc feed from the source cluster
    * `CREATE CHANGEFEED FOR TABLE [source_table] INTO 'http://[cdc-sink-host:port]/target_database/target_schema' WITH updated,resolved`
    * The `target_database` path element is the name passed to the `CREATE DATABASE` command.
    * The `target_schema` element is the name of
      the [user-defined schema](https://www.cockroachlabs.com/docs/stable/create-schema.html) in the
      target database. By default, every SQL `DATABASE` has a `SCHEMA` whose name is "`public`".
    * Be sure to always use the options `updated` and `resolved` as these are required for this to
      work.
    * The `protect_data_from_gc_on_pause` option is not required, but can be used in situations
      where a `cdc-sink` instance may be unavailable for periods longer than the source's
      garbage-collection window (25 hours by default).

## Theory of operation

* `cdc-sink` receives a stream of partially-ordered row mutations from one of more nodes in the
  source cluster.
* The two leading path segments from the URL,
  `/target_database/target_schema`, are combined with the table name from the mutation payload to
  determine the target table:
  `target_database.target_schema.source_table_name`.
* The incoming mutations are staged on a per-table basis into the target cluster, which is ordered
  by the `updated` MVCC timestamps.
* Upon receipt of a `resolved` timestamp, mutations whose timestamps are less that the new, resolved
  timestamp are dequeued.
* The dequeued mutations are applied to the target tables, either as
  `UPSERT` or `DELETE` operations.
* The `resolved` timestamp is stored for future use.

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

```
CREATE TABLE _targetDB_targetSchema_targetTable (
  nanos INT NOT NULL,
logical INT NOT NULL,
 key STRING NOT NULL,
  mut JSONB NOT NULL,
PRIMARY KEY (nanos, logical, key)
)
```

There is also a timestamp-tracking table.

```
CREATE TABLE _timestamps_ (
  key STRING NOT NULL PRIMARY KEY,
  nanos INT8 NOT NULL,
logical INT8 NOT NULL
)
```

## Changefeed replication

```
Usage:
  cdc-sink start [flags]

Flags:
      --batchSize int            default size for batched operations (default 100)
      --bindAddr string          the network address to bind to (default ":26258")
      --conn string              cockroach connection string (default "postgresql://root@localhost:26257/?sslmode=disable")
      --disableAuthentication    disable authentication of incoming cdc-sink requests; not recommended for production.
  -h, --help                     help for start
      --jwtRefresh duration      how often to scan for updated JWT configuration; set to zero to disable (default 1m0s)
      --schemaRefresh duration   how often to scan for schema changes; set to zero to disable (default 1m0s)
      --tlsCertificate string    a path to a PEM-encoded TLS certificate chain
      --tlsPrivateKey string     a path to a PEM-encoded TLS private key
      --tlsSelfSigned            if true, generate a self-signed TLS certificate valid for 'localhost'

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
# cdc-sink started as a background task. Remove the tls flag for CocrkoachDB <= v21.1
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

### Modes

`cdc-sink` supports a number of optional modes for each changefeed. All of these modes can be
combined in a single changefeed.

#### Immediate

Immediate mode writes incoming mutations to the target schema as soon as they are received, instead
of waiting for a resolved timestamp. Transaction boundaries from the source database will not be
preserved, but overall load on the destination database will be reduced.

Immediate mode is enabled by adding the `?immediate=true` query parameter to the changefeed URL.

#### Compare-and-set

A compare-and-set (CAS) mode allows `cdc-sink` to discard some updates, based on version- or
time-like fields.

**The use of CAS mode intentionally discards mutations and may cause data inconsistencies.**

To opt into CAS mode for a feed, add a `?cas=<column_name>` query parameter to the changefeed. The
named field should be of a type for which SQL defines a comparison operation. Common uses would be a
`version INT` or an `updated_at TIMESTAMP` column. A mutation from the source database will be
applied only if the CAS column is strictly greater than the value in the destination database.

Consider a table with a `version INT` column that is used by adding a
`?cas=version` query parameter to the changefeed URL. If `cdc-sink` receives a message to update
some row in this table, the update will only be applied if there is no pre-existing row in the
destination table, or if the update's `version` value is strictly greater than the existing row in
the destination table. That is, updates are applied only if they would increase the `version` column
or if they would insert a new row.

Multiple CAS columns can be specified by repeating the query
parameter: `?cas=version_major&cas=version_minor&cas=version_patch`. When multiple CAS columns are
present, they are compared as a tuple. That is, the second column is only compared if the first
column value is equal between the source and destination clusters. In this multi-column case, the
following psuedo-sql clause is applied to each incoming update:

```sql
WHERE existing IS NULL OR (
  (incoming.version_major, incoming.version_minor, incoming.version_patch) >
  (existing.version_major, existing.version_minor, existing.version_patch)
)
```

All tables in the feed must have all named `cas` columns or the changefeed will fail.

Deletes from the source cluster are always applied.

#### Deadlines

A deadline mode allows `cdc-sink` to discard some updates, by comparing a time-like field to the
destination cluster's time. This is useful when a feed's data is advisory only or changefeed
downtime is expected.

**The use of deadlines intentionally discards mutations and may cause data inconsistencies.**

To opt into deadline mode for a feed, two query parameters must be
specified: `?dln=<column_name>&dli=<interval>`. The `dln` parameter names a column whose values can
be coerced to a `TIMESTAMP`. The `dli` parameter specifies some number of seconds, or an interval
value, such as `1m`.

Given the configuration `?dln=updated_at&dli=1m`, each incoming update would behave as though it
were filtered with a clause such as `WHERE incoming.updated_at > now() - '1m'::INTERVAL`.

Multiple deadline columns may be specified by repeating pairs of `dln` and `dli` parameters.

All tables in the feed must have all named `dln` columns.

Deletes from the source cluster are always applied.

## PostgreSQL logical replication

An alternate application of `cdc-sink` is to connect to a PostgreSQL-compatible database instance to
consume a logical replication feed. This is primarily intended for migration use-cases, in which it
is desirable to have a minimum- or zero-downtime migration from PostgreSQL to CockroachDB.

```
Usage:
  cdc-sink pglogical [flags]

Flags:
  -h, --help                     help for pglogical
      --metricsAddr string       a host:port to serve metrics from at /_/varz
      --publicationName string   the publication within the source database to replicate
      --slotName string          the replication slot in the source database (default "cdc_sink")
      --sourceConn string        the source database's connection string
      --targetConn string        the target cluster's connection string
      --targetDB string          the SQL database in the target cluster to update

Global Flags:
      --logDestination string   write logs to a file, instead of stdout
      --logFormat string        choose log output format [ fluent, text ] (default "text")
  -v, --verbose count           increase logging verbosity to debug; repeat for trace
```

The theory of operation is similar to the standard use case, the only difference is that `cdc-sink`
connects to the source database to receive a replication feed, rather than act as the target for a
webhook.

### Setup

* A review of
  PostgreSQL [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
  will be useful to establish the limitations of logical replication.
* Run `CREATE PUBLICATION my_pub FOR ALL TABLES;` in the source database. The name `my_pub` can be
  changed as desired and must be provided to the `--publicationName` flag. Specifying only a subset
  of tables in the source database is possible.
* Run `SELECT pg_create_logical_replication_slot('cdc_sink', 'pgoutput');` in the source database.
  The value `cdc_sink` may be changed and should be passed to the `--slotName` flag.
* Once the replication slot is created, perform any bulk data migration that needs to occur. The
  replication slot will be pinned to a specific offset within the write-ahead-log. Any changes made
  to the database after the bulk migration occurs will be replayed, allowing `cdc-sink` to catch up
  to the current state of the source database.
* Run `cdc-sink pglogical` with at least the `--publicationName`, `--sourceConn`, `--targetConn`,
  and `--targetDB` flags.

If you pass `--metricsAddr 127.0.0.1:13013`, a Prometheus-compatible HTTP endpoint will be available
at `/_/varz`. A trivial health-check endpoint is also available at `/_/healthz`.

To clean up from the above:

* `SELECT pg_drop_replication_slot('cdc_sink');`
* `DROP PUBLICATION my_pub;`

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

```shell
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

```
cdc-sink make-jwt -a 'database.schema' --claim 
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

* all the limitations from CDC hold true.
    * See <https://www.cockroachlabs.com/docs/dev/change-data-capture.html#known-limitations>
* schema changes do not work,
    * in order to perform a schema change
        1. stop the change feed
        2. stop cdc-sink
        3. make the schema changes to both tables
        4. start cdc-sink
        5. restart the change feed
* constraints on the destination table
    * foreign keys
        * there is no guarantee that foreign keys between two tables will arrive in the correct
          order so please only use them on the source table
    * different table constraints
        * anything that has a tighter constraint than the original table may break the streaming
* the schema of the destination table must match the primary table exactly

## Expansions

While the schema of the secondary table must match that of the primary table, specifically the
primary index. There are some other changes than can be made on the destination side.

* Different and new secondary indexes are allowed.
* Different zone configs are allowed.
* Adding new computed columns, that cannot reject any row, should work.
