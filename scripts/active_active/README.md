# Docker demo of active-active replication

This demo sets up 2 single node clusters and 2 cdc-sink instances to replicate data two ways,
as shown in the diagram.

![active active setup](active-active.png "Demo setup")

It has a simple conflict resolution, where conflicts are stored in dead letter queue table.
We use the KV workload to simulate conflicting or disjoint workloads.

The demo can be started using docker compose, leveraging profiles to start the different components.

## Configuration

You will need to set the `COCKROACH_DEV_LICENSE` and `COCKROACH_DEV_ORGANIZATION` environment variables.

```bash
export COCKROACH_DEV_LICENSE="_your_license_key"
export COCKROACH_DEV_ORGANIZATION="_your_organization_name"
```

Alternatively, create a `.env` file to set the values of the variables.

## Starting the services

Starting the Cockroach clusters, cdc-sink processes and initializing the environment:

```bash
docker compose up --build -d
```

To pass additional arguments to cdc-sink use the `CDC_ARGS` env variable.
`CDC_ARGS` can be also set in the `.env` file.

For instance, to start with immediate mode:

```bash
CDC_ARGS=--immediate docker compose up -d
```

## Monitoring

To start the optional monitoring environment, add `--profile monitor` to the command above:
(if ran again, it will check that the core services are up and healthy)
```bash
docker compose --profile monitor up -d
```

Docker will start Prometheus and Grafana and initialize the monitoring environment to
connect to the Cockroach Clusters and the cdc-sink processes.
The Grafana dashboard will be available on port 3000, unless the `GRAFANA_PORT` environment
variable is set to a different value. The default user,password is `admin`/`cdc-sink`.

Dashboard: http://localhost:3000/d/cdc-sink

## Running the workload

To run the kv workload with conflicts, using the same sequence number for both workloads.

```bash
docker compose run  --rm concurrent_workloads
```

To run the kv workload with no conflicts, use disjoint sequence numbers, by setting
the `START_EAST` or `START_WEST` environment variables.

```bash
START_EAST=100000 docker compose run  --rm concurrent_workloads
```

The kv uses sequential keys, so it's easier to compare the data on the 2 clusters.
It also possible a workload on each cluster separately, using 

```bash
docker compose run  --rm east_workload
```

or

```bash
docker compose run  --rm west_workload
```

There are few variables that can be used to change the behavior of the workload:

- `START_EAST`: the starting sequence for the east cluster. Default: 0.
- `START_WEST`: the starting sequence for the west cluster. Default: 0.
- `DURATION`: the duration of the test. Default: 60s.
- `RATE`: the insert rate. Default: 100 inserts/second.

We are adding 2 columns to each table that we need to replicate.

- `_source` indicates where the change was originally made.
- `_timestamp` is a timestamp in the future to take into account for a window of uncertainty
(for the demo we are using one second in the future).

## Checking results

To check results, use the SQL client on either clusters, by running either the `east_sql`
or `west_sql` services.

```bash
docker compose run --rm -it west_sql 
```

To view the content of the kv table, including the columns used by the cdc-sink to perform replication:

```sql
root@west:26257/defaultdb> SELECT *,_source,_timestamp from kv.kv limit 10
```

To check the content of the DLQ table:
```sql
root@west:26257/defaultdb> select * from kv.cdc_sink_dlq limit 10;
```

## Clean up

To remove all the containers, volumes from the docker environment:

```bash
docker compose --profile monitor --profile workload --profile sql down -v
```

## Implementation Notes

The flow control and conflict resolution are implemented by providing a user script
to cdc-sink. As a convenience, we provide a module `repl.ts` to setup the replication logic,
on each cdc-sink process. The module must be available in the directory where cdc-sink starts.

Once configured, cdc-sink will:

- discard any incoming messages that have in the `_source` column the value of the destination.
- if there is a conflict (e.g. there is already a row with the same key in the destination table),
then check if the `_timestamp` of the incoming row is newer:
  - if it is, update the row;
  - if not, send the row to the DLQ table.

```javascript

import * as api from "cdc-sink@v1";
import { Document, Table } from "cdc-sink@v1";

export function replicateTo(region: string, schema: string, tables: string[]) {
    console.log("configureSource " + schema)
    api.configureSource(schema, {
        dispatch: (doc: Document, meta: Document): Record<Table, Document[]> => {
            console.trace(JSON.stringify(doc), JSON.stringify(meta));
            let ret: Record<Table, Document[]> = {};
            if (doc._source !== region) {
                console.log("Processing " + doc.k + "(" + typeof doc.k + "):" + doc._source)
                ret[meta.table as string] = [doc];
            } else {
                console.log("Skipping " + doc.k + ":" + doc._source)
            }
            return ret
        },
        deletesTo: ""
    })

    for (let table of tables) {
        console.log("configureTable " + table)
        api.configureTable(table, {
            cas: ['_timestamp'],
            merge: api.standardMerge(() => ({ dlq: table }))
        })}
}

```

On the `east-west` cdc-sink process, we configure replication by calling:

```javascript
import {replicateTo} from "repl";
replicateTo('west','kv.public', ['kv.public.kv'])
```
