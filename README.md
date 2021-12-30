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

## Flags

```
Usage:
  cdc-sink start [flags]

Flags:
      --batchSize int            default size for batched operations (default 1000)
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

## Example

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

## Security Considerations

At a high level, `cdc-sink` accepts network connections to apply arbitrary mutations to the target
cluster. In order to limit the scope of access, [JSON Web Tokens](https://jwt.io) are used to
authorize incoming connections and limit them to writing to a subset of the SQL databases or
user-defined schemas in the target cluster.

A minimally-acceptable JWT claim is shown below. This must include the standard `jti` token
identifier field, and a custom claim for cdc-sink. This custom claim is defined in a globally-unique
namespace and most JWT-enabled authorization providers support adding custom claims. It contains a
list of one or more target databases or user-defined schemas. Wildcards are supported.

```json
{
  "jti": "a25dac04-9f3e-49c1-a068-ee0a2abbd7df",
  "https://github.com/cockroachdb/cdc-sink": {
    "sch": [
      [
        "database",
        "schema"
      ],
      [
        "another_database",
        "*"
      ],
      [
        "*",
        "required_schema"
      ],
      [
        "*",
        "*"
      ]
    ]
  }
}
```

The JWT tokens must be signed with RSA or EC keys. HMAC and  `None` signatures are rejected.
The `_cdc_sink.jwt_public_keys` table should be populated with one or more public keys used to sign
the tokens. If a specifc token needs to be revoked, its `jti` value can be added to
the `_cdc_sink.jwt_revoked_ids` table. These tables will be re-read every minute by the `cdc-sink`
process. A `HUP` signal can be sent to force an early refresh.

The encoded token is provided to the `CREATE CHANGEFEED` using
the `WITH webhook_auth_header='Bearer <encoded token>'`
[option](https://www.cockroachlabs.com/docs/stable/create-changefeed.html#options). In order to
support older versions of CockroachDB, the encoded token may also be specified as a query
parameter `?access_token=<encoded token>`. Note that query parameters may be logged by
intermediate loadbalancers, so caution should be taken.

### Token Quickstart

```shell
# Generate a EC private key using OpenSSL.
openssl ecparam -out ec.key -genkey -name prime256v1

# Write the public key components to a separate file.
openssl ec -in ec.key -pubout -out ec.pub

# Generate a token which can write to the ycsb.public schema.
# The key can be decoded using the debugger at https://jwt.io.
# Add the contents of out.jwt to the CREATE CHANGEFEED command:
# WITH webhook_auth_header='Bearer <out.jws>'
cdc-sink make-jwt -k ec.key -a ycsb.public -o out.jwt

# Upload the public key for cdc-sink to find it.
cockroach sql -e "INSERT INTO %s.%s (public_key) VALUES ('$(cat ec.pub)')"

# Reload configuration, or wait a minute.
killall -HUP cdc-sink
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
