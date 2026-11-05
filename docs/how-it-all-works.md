# How it all works: the Egypt Water Intelligence pipeline, from the ground up

This document explains **what every part of the system does, why it is there, and how it talks to the parts
around it**. It is not a code walkthrough. It builds up the concepts gradually: first the basic building blocks
(containers, events, topics, files, tables), then each tool, then how they connect, and finally what happens when
things go wrong. The aim is that you can answer "how does X work?" and "why did you do it that way?" for any part.

How to read it: Part 1 is the whole story in five minutes. Parts 2 to 6 go deep, one layer at a time. Part 7 maps
every connection. Part 8 follows one reading through the whole system. Part 9 covers failures. Part 10 is a list
of likely questions with short answers. A glossary closes the document.

---

## Part 1. The whole story in five minutes

We want to find water leaks in a city's network **while they happen**, and prove that the system finds them.
Real utility data never tells you where the leaks really were, so we cannot grade a detector on it. So:

1. **We simulate a city.** A generator uses a real hydraulic engine (EPANET) to simulate six districts and 1,500
   customer meters for 25 days, minute by minute. We plant 13 incidents (leaks, look-alikes) and inject 1,353 data
   faults (blank values, repeated messages). The "answer key" (when each incident really happened) is kept aside.
2. **The readings are published as a stream** into **Kafka**, the way real sensors would send them: 3.8 million
   messages on two topics, one for district sensors (every minute) and one for customer meters (every 15 minutes).
3. **Two paths read that stream:**
   - **The streaming path (Flink)** watches the district sensors continuously, builds 15-minute summaries per
     district, compares each district with its neighbours and raises alerts within about 30 minutes of an incident.
   - **The batch path** starts with an **archiver** that copies every Kafka message into files in **MinIO**
     (object storage). Once a dataset is proven complete, **Spark** reads those files, cleans them, computes the
     water balance (water in vs water metered) and builds analytical tables.
4. **Both paths write into Iceberg tables** stored in MinIO. A small **Postgres** database (the "catalog") records
   where each table currently is, so **Spark** can write tables and **Trino** can read the very same tables with SQL.
5. **Trino** joins the two paths into **marts** (ready-made tables for dashboards), exported to **Power BI**.
6. **A live bridge** copies Flink's results into Postgres as they happen, so **Grafana** can show a live board.
7. **Dagster** runs all of this in order and, more importantly, **proves** each step is complete before the next one
   starts, so a re-run never duplicates or loses data.
8. Only at the very end does an **evaluation** open the answer key and grade the alerts: 12 of 12 detectable
   incidents found, one false alarm.

```
Generator ──► Kafka ──┬──► Flink (4 jobs) ──► Kafka (alert topics) ──┬──► live bridge ──► Postgres "live" ──► Grafana
                      │                                              └──► Spark bounded load ──┐
                      └──► Archiver ──► MinIO (archive files) ──► Spark batch ─────────────────┤
                                                                                               ▼
                                         Iceberg tables in MinIO  (catalog: Postgres)  ◄── Trino ──► Power BI
                     Dagster (on the host) starts, checks and re-runs every step above
```

---

## Part 2. The ground floor: how the system is run

### 2.1 Containers and images

Every service (Kafka, MinIO, Flink, Postgres, Trino, Spark, Grafana, the archiver, the live bridge) runs in its own
**container**. A container is an isolated process with its own filesystem, built from an **image**: a frozen
package of an operating-system layer, the program and its libraries. Some images are used as published (Postgres,
Trino, Kafka, MinIO, Grafana); four are built by us from folders in the repository (the generator/archiver image, the
Flink image, the Spark image and the live-bridge image), because they need our code or extra libraries inside.

Why containers: everyone on the team gets the same versions with one command, nothing is installed on the laptop,
and the whole stack can be thrown away and recreated.

### 2.2 Docker Compose: one file describes the whole system

`docker-compose.yml` lists every service: which image, which settings, which ports, which storage, and which other
services it waits for. One command (`docker compose up -d`) starts them all.

Important ideas in that file:

- **Long-running services vs one-shot tools.** Kafka, MinIO, Flink, Postgres, Trino, Grafana, the archiver and the
  live bridge run permanently. The **generator** and **Spark** are marked as *tools*: they are not started with the
  stack; each use starts a fresh container that does one job and exits (`docker compose run --rm ...`). That is why
  a Spark job is a short-lived container, and why its live web page (port 4040) only exists while a job runs.
- **The `init` service** runs once at start-up: it creates the two input Kafka topics and the MinIO buckets, then
  exits. Other services wait until it has "completed successfully".
- **Health checks and start order.** Kafka, MinIO, Postgres and Trino each have a health check (a small command that
  says "I am ready"). Services that depend on them start only when they are healthy. This avoids "connection
  refused" races at start-up.
- **Restart policy.** The archiver and the live bridge have `restart: unless-stopped`: if they crash, Docker restarts
  them automatically. Both are safe to restart because of how they commit their progress (explained later).

### 2.3 Networking: two names for the same service

Compose puts all containers on one private network where **each service is reachable by its service name**. Inside
that network, Kafka is `kafka:9092`, MinIO is `minio:9000`, Postgres is `catalog-db:5432`, Trino is `trino:8080`.

Your laptop (the "host") is outside that network. To reach a service from the host, Compose **publishes a port**:
it forwards a port on `localhost` to the container. So the same service has two addresses:

| Service | Inside the Docker network | From your laptop |
|---|---|---|
| Kafka | `kafka:9092` | `localhost:29092` |
| MinIO (S3 API / web console) | `minio:9000` | `localhost:9000` / `localhost:9001` |
| Flink (web + REST API) | `flink-jobmanager:8081` | `localhost:8081` |
| Trino | `trino:8080` | `localhost:8082` |
| Kafka UI | `kafka-ui:8080` | `localhost:8080` |
| Grafana | `grafana:3000` | `localhost:3001` |
| Spark UI / history | `spark:4040` / `spark-history:18080` | `localhost:4040` / `localhost:18080` |
| **Postgres (`catalog-db`)** | `catalog-db:5432` | **not published**: only reachable from inside |

Kafka needs a special trick for this. A Kafka client first asks the broker "where are you?", and the broker answers
with an *advertised address*. If it always answered `kafka:9092`, your laptop could not use it (that name does not
exist outside Docker). So Kafka has **two listeners**: containers connect to `kafka:9092` and are told `kafka:9092`;
the host connects to `localhost:29092` and is told `localhost:29092`. This is why Dagster, which runs on the host,
uses port 29092.

Postgres is deliberately **not** published: only Spark, Trino, the live bridge and Grafana (all containers) need it.

### 2.4 Volumes: where data survives

A container's own filesystem is thrown away when the container is removed. Anything that must survive is kept in a
**named volume** (managed by Docker) or a **bind mount** (a folder of the repository mapped into the container).

- Named volumes: `kafka-data` (Kafka's logs), `minio-data` (all buckets), `catalog-db-data` (Postgres),
  `flink-state` (checkpoints and savepoints), `spark-events` (finished Spark job history), `grafana-data`.
- Bind mounts: `Generator/output` (generated datasets), `flink/jobs` (the job files Flink runs), the Trino catalog
  configuration, Grafana's provisioning files, `dashboards/data` (Parquet exports for Power BI).

`docker compose down` stops everything but **keeps volumes**, so data survives a restart. `down -v` would delete them.

### 2.5 Where Dagster runs

Dagster is **not** a container. It runs on the laptop in its own Python environment (managed by `uv`), started with
`.\run.ps1 dagster`, with its web UI on `localhost:3000`. It controls the containers in two ways:

- by typing the same `docker compose ...` commands a person would (for example to start a Spark job), and
- by talking to the services' published ports (Kafka on 29092, MinIO on 9000, Flink's REST API on 8081, Trino on
  8082) to check their state.

Dagster's own run history lives in `orchestration/.dagster_home` (a small local database), but the facts that make
re-runs safe are stored next to the data in MinIO and Iceberg (Part 6).

---

## Part 3. The data itself

### 3.1 Events and the two streams

An **event** is one reading, written as one JSON object. There are two kinds:

- **District sensor event**, every minute, per district (6 districts × 36,000 minutes = 216,000 events): how much
  water entered the district through its inlet pipe (flow and volume) and the pressure inside the district.
- **Meter reading event**, every 15 minutes, per customer meter (1,500 meters × 2,400 intervals = 3,600,000 events):
  how much water that customer consumed in those 15 minutes.

These two have different **grains** (the time span and entity one row describes): one district for one minute vs one
meter for 15 minutes. You cannot join them on equal timestamps; you have to aggregate them to a common grain first
(district × 15 minutes). That is exactly what the batch water balance does.

### 3.2 The fields that matter for engineering

- **`event_id`**: `run : stream : entity : minute`, for example `run-72b96af88a342375:meter:M000001:000000`. It is
  unique per real reading and stable, so if the same message arrives twice, it is provably the same reading. It is
  the **deduplication key** everywhere.
- **`run_id`**: which generated dataset the event belongs to. It is the first 16 hex characters of a SHA-256 hash of
  the dataset's configuration, so the same configuration always gives the same run ID.
- **Two clocks**: `event_time_utc` (when the measured thing happened) and `availability_time_utc` (when a consumer
  could first see it). Streaming logic must use event time, not the moment a message happened to arrive.
- **Interval**: `interval_start_utc` / `interval_end_utc` state exactly which period a value covers.
- **`quality_code`**: `valid` on clean data; anything else marks a suspicious reading.
- **`schema_version`** `1.0.0`: lets a consumer reject data it does not understand.

### 3.3 Three layers of data

The generator writes three sets of files per run:

1. **Public feed**: what a perfect network would report (3,816,000 events).
2. **Delivery feed**: the public feed plus injected faults: 828 values blanked (set to null) and 525 messages
   repeated (3,816,525 events). **This is the only data the pipeline ever receives.**
3. **Restricted truth**: when each of the 13 incidents started and ended, and a record for every injected fault with
   the result the pipeline is expected to produce. It goes to a separate bucket and only the final evaluation reads it.

Faults are injected on fixed, prime intervals (every 4,999th meter reading loses its volume, every 7,919th is sent
twice, and similarly for districts), so the exact counts are known and no reading is ever both blanked and repeated.

### 3.4 Why the generated data is trustworthy

- **Physics, not noise.** EPANET solves the whole pipe network every simulated minute. A leak is modelled as an
  opening in a pipe (an "emitter"), so flow rises and pressure falls together, as in reality.
- **Pressure-dependent demand.** Below 5 m of pressure a customer gets nothing, from 20 m everything they ask for, in
  between a square-root curve. So supply problems change what meters record, like in a real network.
- **Self-check.** In every district and minute, water in must equal water used plus water leaked. The worst error over
  the whole run is 7.3 × 10⁻⁸ m³, far inside the 10⁻⁴ tolerance.
- **Reproducible.** One configuration file plus a fixed random seed always rebuilds the same dataset and run ID.

---

## Part 4. Moving the data: Kafka, MinIO and the archiver

### 4.1 Kafka: a durable, replayable log

Think of Kafka as a set of **append-only logs**. Producers append messages to the end; consumers read them in order.
Unlike a classic message queue, **reading does not delete anything**: messages stay for the retention period (7
days by default), so many consumers can read the same data independently, and a consumer can go back and re-read.

Key concepts:

- **Broker**: the Kafka server. We run one, in **KRaft mode**, which means it manages its own cluster metadata
  instead of needing a separate ZooKeeper service.
- **Topic**: a named log, for example `water.district-sensors.v1`. The `.v1` is part of the name so a future schema
  change can use a new topic without breaking old consumers.
- **Partition**: a topic is split into partitions for parallelism; ordering is guaranteed only within a partition.
  Our topics have **one partition**, which keeps ordering simple on a single laptop.
- **Offset**: the position of a message inside a partition (0, 1, 2, ...). Offsets never change and never repeat, so
  an offset range is an exact description of "which messages".
- **Key**: each message has a key; we use the `event_id`. It identifies the message for tracing. (It does *not*
  remove duplicates; Kafka happily stores two messages with the same key.)
- **Producer acknowledgements (`acks=all`)**: the publisher waits until Kafka confirms each message is stored before
  counting it as sent. Slower (about an hour for 3.8 M messages on a laptop), but nothing is silently lost.
- **Consumer group and committed offset**: a consumer belongs to a named group. Kafka remembers, per group, the
  **committed offset**: how far that group has confirmed it has processed. After a restart, the group continues from
  there. Each of our consumers (the archiver, each Flink job, the live bridge) has its **own** group, so each one sees
  every message regardless of the others.
- **At-least-once**: if a consumer processes messages but crashes before committing, it will process them again
  after a restart. Nothing is lost, but some messages can be processed twice. That is why deduplication by
  `event_id` (and by `alert_id` later) appears everywhere downstream.

Topics in the system:

| Topic | Written by | Read by |
|---|---|---|
| `water.district-sensors.v1` | generator (publisher) | archiver, Flink validation job |
| `water.meter-readings.v1` | generator (publisher) | archiver only (Flink uses districts only) |
| `water.district-sensors.clean.v1` | Flink validation | Flink features |
| `water.dead-letter.district.v1` | Flink validation | Spark bounded load |
| `water.district-features.15m.v1` | Flink features | Flink alerts, live bridge, Spark load |
| `water.district-relative.15m.v1` | Flink alerts | Flink level shift, live bridge, Spark load |
| `water.district-alerts.v1` | Flink alerts and level shift | live bridge, Spark load |

A parallel set prefixed `demo.` exists for the live demo, so the demo can be reset without touching the main run.

### 4.2 The publisher

The generator's publisher reads the two delivery files and sends their records in **availability-time order** (the
order they would have arrived in reality), waiting for Kafka's acknowledgement of each. Our run was interrupted once
(the broker briefly lost "leadership" of a partition); instead of republishing everything, Dagster proved that Kafka
held an exact prefix of the expected sequence (same count per topic, same last message) and sent only the rest.

### 4.3 MinIO: object storage

MinIO is an **object store** that speaks the same API as Amazon S3. You put whole files ("objects") into
**buckets** under a **key** (a name like `events/water.meter-readings.v1/run-72b9.../part-0-0-9999.ndjson.gz`).
There are no real folders; slashes in keys only look like folders in the console. Objects are written whole and
never edited in place.

Why object storage: it is cheap, scales to huge amounts of data, many engines can read it, and it is how data lakes
work in the cloud. Using the S3 API means the same code would work on real AWS S3.

Four buckets, each with one job:

| Bucket | Contains | Who writes | Who reads |
|---|---|---|---|
| `water-raw` | the run's reference files (districts, meters) and the `_RUN_COMPLETE` marker | Dagster | Spark batch |
| `water-restricted` | the answer key (scenario and fault truth) | Dagster | the evaluation only |
| `water-events` | the Kafka archive, the archive manifest, publication state | archiver, Dagster | Spark batch, Dagster |
| `water-warehouse` | all Iceberg table files (data + metadata) | Spark, Trino | Spark, Trino |

### 4.4 The archiver: Kafka → MinIO

The archiver is a permanent container (built from the generator image) with its own consumer group. It buffers
messages and, every 10,000 messages or 30 seconds (or 32 MiB), writes one compressed file of newline-delimited JSON
(**NDJSON**, one event per line, gzip) to `water-events`. Each file name contains the **exact Kafka offset range** it
holds: `part-<partition>-<first offset>-<last offset>.ndjson.gz`.

Two rules make it trustworthy:

1. **Commit after upload.** It commits its Kafka offset only after the file is safely stored in MinIO. If it crashes
   in between, it re-reads those messages after restart: nothing lost (possibly one batch written twice).
2. **Offsets in file names.** Anyone can check which part of the stream each file covers, so completeness can be
   **proven**, not assumed (Part 6).

Why an archive at all, when Kafka already keeps the data? Kafka keeps messages for 7 days and is a transport, not a
long-term store. The archive is the permanent, complete copy that batch processing can re-read at any time, and it
guarantees the batch path sees exactly what travelled through Kafka (nothing bypasses the stream).

---

## Part 5. Processing the data

### 5.1 Flink: stream processing

**What Flink is.** A stream processor: it runs jobs that read unbounded streams, keep **state** (memory of what it
has seen), and continuously write results. Our jobs are written in **Flink SQL** through PyFlink (Python), so each
job is mostly SQL statements over "tables" that are actually Kafka topics.

**How it runs.** Two containers:
- the **JobManager** coordinates: accepts jobs, schedules them, triggers checkpoints, serves the web UI (8081);
- the **TaskManager** does the work in **task slots** (we have 4, one per job). **Parallelism** 2 means an
  operator can run as two parallel copies.

**Event time and watermarks.** Each record carries its own time (`event_time_utc`). A 15-minute window should close
when all readings *for* that period have arrived, not after 15 minutes of wall-clock time (data can be replayed fast
or arrive late). Flink tracks a **watermark**: a moving statement "I believe all events up to time T have arrived".
Our watermark is "latest event time minus 1 minute". When the watermark passes the end of a window, the window is
closed and its result emitted. An **idle timeout** stops a quiet (empty) source from holding the watermark back.

**Tumbling windows.** Fixed, non-overlapping time buckets: 00:00–00:15, 00:15–00:30, and so on. Each district
gets one result row per window.

**Checkpoints and savepoints.** Every 30 seconds Flink takes a **checkpoint**: a consistent snapshot of all job
state plus the Kafka offsets it has read, stored in the `flink-state` volume. If a job fails, it restarts from the
last checkpoint, as if the failure never happened (except that output written since the checkpoint may be written
again: at-least-once). A **savepoint** is a checkpoint you trigger by hand, used to stop and later resume a job
safely (the demo uses them).

**State TTL.** State is kept only as long as needed (for example 10 minutes for "have I seen this event_id?",
because duplicates arrive close together), so memory stays bounded.

**The four jobs, chained through Kafka topics:**

1. **Validation.** Reads the raw district topic *as plain text*, so even broken JSON is captured instead of crashing
   the job. Nine checks run in order; the first one that fails puts the record on the **dead-letter topic** with a
   reason (malformed JSON, wrong schema version, missing field, invalid district ID, unknown quality code, bad
   timestamp, interval not exactly 60 s, non-numeric value, negative value). A **null pressure is not rejected**: it
   stays null and is flagged, so it can never look like a pressure drop. Repeats are removed by `event_id`. Result:
   216,071 in, 216,000 clean, 71 repeats removed, 0 rejected.
2. **Features.** 15-minute tumbling windows per district: average flow, inlet volume, minimum and average pressure,
   number of readings (15 expected), number of null pressures and flagged readings. Result: 14,394 windows.
3. **Alerts (rules R1–R3).** For each window, each district's flow and pressure are divided by the **median of the
   other five districts** in the same window ("relative flow"). Districts share the same daily rhythm (morning peak,
   night low), so the ratio removes the rhythm and leaves the anomaly.
   - **R1 flow anomaly**: relative flow more than 10% away from 1 for **two consecutive windows**.
   - **R2 supply disturbance**: relative flow below 0.90 **and** relative pressure below 0.80 for two windows; checked
     first, so a supply drop is never called a leak.
   - **R3 data quality**: not 15 readings in the window, a null pressure, or a flagged reading; raised instantly.
   - Incidents have a **lifecycle**: an `open` alert on the second suspicious window and a `closed` alert when the
     district returns to normal. Requiring two windows means one noisy window never raises an alarm (cost: about 30
     minutes of delay).
4. **Level shift (R4).** Slow, persistent losses (a few percent) never cross the 10% rule. R4 compares each district's
   **daily** average relative flow with the median of its own previous 6 days, ignoring windows R1/R2 already
   explained, and alerts on a sustained step above 3% (once per shift). Cost: about one day of delay.

Why separate jobs connected by topics, instead of one big job: each step can be tested, inspected and replayed on
its own, and the intermediate topics feed other consumers (the live board, the lakehouse).

Note: Flink processes **only the district stream**. Customer meters are used by the batch path, which can afford to
wait for complete intervals.

### 5.2 The lakehouse idea

A **data warehouse** stores clean tables with strong guarantees but in a proprietary system. A **data lake** stores
any files cheaply in object storage but has no transactions: two writers can corrupt each other, and a reader can see
half-written data. A **lakehouse** puts a **table format** on top of the lake to get warehouse-like guarantees on
cheap object storage. We use **Apache Iceberg** as that table format.

### 5.3 Parquet: the file format

Iceberg's data files are **Parquet**: a columnar file format. Values of one column are stored together, compressed,
with statistics (min/max) per chunk. A query that needs 3 columns reads only those 3, and can skip chunks whose
min/max cannot match the filter. This is why analytical queries on millions of rows are fast.

### 5.4 Iceberg: how a pile of files becomes a table

An Iceberg table is a set of files in MinIO organised in layers:

1. **Data files** (Parquet): the rows.
2. **Manifest files**: lists of data files, with each file's partition and column statistics.
3. **Manifest list**: the list of manifests that make up one version of the table.
4. **Metadata file** (JSON): the table's schema, partitioning, properties, and the list of **snapshots** (versions),
   each pointing to a manifest list.

A write never modifies existing files. It writes new data files, new manifests and a **new metadata file**, then
**atomically switches the table's "current metadata" pointer** from the old metadata file to the new one. Readers
always start from the current pointer, so they see either the complete old version or the complete new version,
never a half-written mix. That pointer switch is the **commit**.

Consequences you can mention:
- **ACID-style commits** on object storage: a failed write leaves no trace in the table.
- **Snapshots and time travel**: old versions remain until expired, so you can query the table as of an earlier
  snapshot (`table$snapshots` shows the history).
- **Hidden partitioning**: our tables are partitioned by `run_id`, so each generated run's rows live in their own
  files and can be replaced without touching other runs.
- **Schema evolution**: columns can be added safely because columns are tracked by ID, not by position.

### 5.5 The catalog: the one thing that must be shared (and where JDBC comes in)

If the commit is "switch the pointer to the current metadata file", **where is that pointer kept**? That is the job of
the **catalog**: a small service that maps each table name (`water.gold.fact_water_balance_15m`) to its current
metadata file, and performs the switch atomically so two writers cannot both win.

Catalogs come in several kinds: a Hive Metastore, a REST catalog service, a cloud catalog (AWS Glue), or a **JDBC
catalog**, which simply keeps the pointers in a normal relational database. **We use a JDBC catalog backed by
Postgres.**

**What JDBC is.** JDBC (Java Database Connectivity) is the standard way Java programs talk to relational databases:
the program loads a **driver** for the database (here the PostgreSQL driver) and connects with a **JDBC URL**, here
`jdbc:postgresql://catalog-db:5432/iceberg` (protocol `jdbc:postgresql`, host `catalog-db`, port 5432, database
`iceberg`), plus a user and password. Spark and Trino are both Java programs, so both can use the same JDBC catalog
with nothing extra to run. That is why the Spark image includes the PostgreSQL JDBC driver, and why Trino's
configuration says `iceberg.catalog.type=jdbc`.

**What is inside the catalog database.** When Postgres first starts, an initialisation script creates two tables:

- `iceberg_tables`: one row per table: catalog name, namespace, table name, **`metadata_location`** (the pointer to
  the current metadata file in MinIO) and `previous_metadata_location`.
- `iceberg_namespace_properties`: one row per namespace property (namespaces are like schemas: `analytics`, `gold`,
  `streaming`).

**What a commit looks like in Postgres.** Iceberg runs, in effect, "update the row for this table, set
`metadata_location` to the new file, **only if** it is still the old file I started from". If another writer committed
first, the condition fails, nothing changes, and the writer retries on top of the newer version. This
**compare-and-swap** on a single row is what makes commits atomic, and Postgres transactions make it safe.

**How Spark and Trino share it.** Both are configured with the same three things:

| Setting | Value | Meaning |
|---|---|---|
| catalog name | `water` | the name both engines use for this catalog; tables are `water.<namespace>.<table>` |
| catalog database | `jdbc:postgresql://catalog-db:5432/iceberg` | where the pointers live |
| warehouse | `s3://water-warehouse/` | where table files are created, in MinIO |

Both also know how to reach MinIO: the S3 endpoint `http://minio:9000`, the access keys, and **path-style access**
(MinIO serves buckets as `minio:9000/bucket/key` rather than `bucket.minio:9000/key`). One quirk: the S3 client
library requires a "region" even though MinIO has none, so a placeholder `us-east-1` is set.

Result: **a table Spark commits is visible to Trino at once**, with no copying and no export, because both read the
same pointer from Postgres and the same files from MinIO.

Why JDBC instead of the REST catalog the earlier attempt used: it needs no extra service (we already need Postgres
for the live board), and both engines support it out of the box. Trade-off: every engine must have network access
and credentials to Postgres; a REST catalog would centralise that behind an API.

### 5.6 Spark: batch processing

**What Spark is.** A distributed engine for processing large datasets in parallel. A **driver** plans the work; it
splits data into **partitions** and runs **tasks** on **executors**. Here everything runs inside one container in
local mode (driver and executors share the machine), but the same code would run on a cluster.

Concepts worth knowing:
- **DataFrames** are tables in memory; transformations (filter, join, group) are **lazy**: Spark builds a plan and
  runs it only when an action (write, count) needs the result. Each action becomes a **job**, split into **stages**.
- A **shuffle** moves rows between tasks so that rows with the same key meet (needed for joins and group-bys). It is
  the expensive part of most jobs.
- A **broadcast join** sends a small table (6 districts, 1,500 meters) to every task, so the large table never needs a
  shuffle for that join.
- The **Spark UI** (port 4040) shows jobs and stages while a job runs; the **history server** (18080) shows finished
  jobs from the event logs stored in a volume.

**Spark is used three times:**
1. **The batch build** (`build_batch.py`): cleaning, water balance, gold tables (5.7).
2. **The bounded streaming load** (`load_streaming.py`): copies Flink's results from Kafka into Iceberg (5.8).
3. **The dashboard export** (`export_dashboard.py`): writes Parquet files for Power BI into `dashboards/data`.

### 5.7 The batch build, step by step

The unit of new data is **one generated run**: a fixed, known set of Kafka offsets.

1. **Gates.** The run is processed only if `_RUN_COMPLETE` exists in `water-raw` **and** the archive manifest exists in
   `water-events` (written by Dagster only after the archive is proven complete). Otherwise the job stops with a clear
   error rather than reading partial data.
2. **Fingerprint.** It computes a fingerprint of every input file (sizes, ETags, timestamps) plus a pipeline version and
   compares it with the one stored in the `batch_manifest` table. Unchanged → skip in seconds. Changed → refuse unless
   explicitly told to reprocess. New → build.
3. **Read.** Only the archive files listed in the manifest (never "everything in the bucket") and the reference files.
4. **Clean.** Reject malformed or invalid records (kept with the reason in `rejected_events`), collapse identical
   repeats, stop the build if two *different* records share an `event_id` (that would be a real data problem).
   Result: 828 rejected (the blanked values), 525 repeats collapsed, 3,815,172 accepted.
5. **Water balance**, per district and 15 minutes: water in (sum of the 15 one-minute inlet volumes) minus water
   metered (sum of that district's meters) = **unmetered water** (lost or unbilled). An interval is "complete" only
   if all 15 minutes and every expected meter are present; incomplete intervals get no gap value rather than a
   misleading one.
6. **Gold star schema.** Dimensions (`dim_district`, `dim_meter`, `dim_interval` with Cairo local time), facts at
   15-minute grain (`fact_water_balance_15m` per district, `fact_meter_consumption_15m` per meter) and daily tables.
7. **Write.** Each table is written with **partition overwrite**: only this run's `run_id` partition is replaced;
   other runs are untouched. The `batch_manifest` row is set to `completed` **last**, so a crash half-way leaves it at
   `processing` and nobody trusts the half-written run.

Why partition overwrite rather than MERGE: a run never changes after it lands, so replacing its whole partition is
the simplest atomic write. MERGE is for updating individual rows that change over time.

One performance detail: every write holds a single `run_id`, so Iceberg's default "redistribute rows by partition
before writing" would push all rows through one task for nothing; it is switched off (`write.distribution-mode=none`).

### 5.8 Getting Flink's results into Iceberg

Flink writes only to Kafka. Instead of adding an Iceberg writer inside the Flink jobs (new libraries and changes to
working jobs), a **bounded** Spark job copies the results:

1. When Dagster proves Flink has processed everything, it records the **end offset of each Flink output topic** for
   this run ("frozen offsets"). This matters because Flink's rows do not carry a `run_id`: anything published later
   must not be mixed in.
2. Spark reads each topic **from the beginning up to the frozen offset** (a bounded read, not a stream).
3. It removes repeats (Flink's output is at-least-once): alerts by `alert_id`, windows by district + window start.
4. It adds the `run_id` and writes `water.streaming.fact_district_alert` (140 rows), `fact_district_window_15m`
   (14,394) and `fact_dead_letter` (0), replacing only this run's partition.

Limitation: because Flink rows have no run ID, one set of Kafka topics carries the streaming results of one run.

### 5.9 Trino: SQL over the lakehouse

**What Trino is.** A distributed SQL query engine that **stores nothing itself**. It reads data where it lives through
**connectors**. A **catalog** in Trino is one configured connector; our file `water.properties` defines a catalog
named `water` using the Iceberg connector with the JDBC catalog and MinIO settings from 5.5. So in Trino you write
`SELECT ... FROM water.gold.fact_water_balance_15m`, exactly the same name Spark uses.

**The marts.** Four plain SQL files, run by Dagster through Trino's REST API, each rebuilt per run with "delete this
run's rows, then insert":

- `mart_incidents`: pairs Flink's open/closed alerts into one row per incident (start, end, detection delay, peak).
- `mart_incident_balance`: **joins the two paths**. District sensors alone cannot tell a burst from a demand surge
  (inflow rises in both). For every incident, the batch meter data says whether unmetered water rose (`loss_like`),
  metered consumption rose (`demand_like`) or inflow dropped (`supply_like`). Every leak, surge and supply drop was
  typed correctly, without the answer key.
- `mart_district_daily_overview`: one row per district and day for the dashboard.
- `mart_district_hourly_profile`: a typical week per district (hour-of-week heatmap).

Marts are **tables, not views**, so the dashboard reads finished numbers and every figure traces to one SQL file.

### 5.10 Postgres: one container, two jobs ("where is live Postgres?")

There is **one Postgres container**, the Compose service **`catalog-db`** (image `postgres`), with its data in the
`catalog-db-data` volume, reachable only inside the Docker network at `catalog-db:5432`. There is **no separate
"live Postgres" service**. Inside that one database (`iceberg`) there are two separate areas:

| Area | What it holds | Who writes | Who reads |
|---|---|---|---|
| Iceberg catalog tables (`iceberg_tables`, `iceberg_namespace_properties`) | the pointers to every Iceberg table's current metadata | Spark, Trino (via JDBC) | Spark, Trino |
| **schema `live`** (`live.alerts`, `live.relative`, `live.features`, `live.throughput`) | a live, serving copy of Flink's results | the live bridge | Grafana |

A **schema** in Postgres is a namespace inside a database, like a folder for tables. Keeping the live tables in their
own schema separates them from the catalog tables, and lets us give Grafana access to `live` only.

**Users (roles):**
- `iceberg` owns the database; Spark, Trino and the live bridge connect as this user.
- `grafana_live` is a **read-only** role created by the live bridge, with permission to read schema `live` and
  nothing else. Grafana connects as this role, so a dashboard can never change data or see the catalog.

Why one Postgres for both: fewer moving parts on a laptop; schemas and roles keep the two uses apart. In production
you would likely separate them (the catalog is critical metadata; the live store has a different load).

Important distinction: the **Iceberg tables' data is not in Postgres**. Postgres holds only the small pointers; the
rows are Parquet files in MinIO. The **live tables' data is in Postgres**, because a live board needs fast small
queries every few seconds.

### 5.11 The live bridge: Kafka → Postgres

A small always-on Python service (its own container, `live-bridge`, built from the `live/` folder):

- It joins Kafka as consumer group `ewi-live-bridge` and **subscribes by pattern** to Flink's output topics (alerts,
  relative, features), both the main topics and the `demo.` ones, so recreated demo topics are picked up
  automatically.
- Every second it reads a batch of messages and **upserts** them into the `live` tables: "insert, or if a row with
  this key already exists, update it" (`ON CONFLICT ... DO UPDATE`, or `DO NOTHING` for alerts). The keys are the
  natural keys: `alert_id` for alerts, district + window start for windows. So reading the same message twice can
  **never** create a duplicate row. This is what makes an at-least-once source safe to copy.
- Every 2 seconds it records the **end offset** (total message count) of the main topics in `live.throughput`. The
  difference between two samples divided by the time gives **events per second** on the board.
- At start-up it creates the `live` schema, its tables and the `grafana_live` role if they do not exist.

Kafka remains the **system of record**; the live tables are a disposable serving copy that can be rebuilt by
re-reading the topics.

### 5.12 Grafana: the live board

Grafana is a dashboard server (container `grafana`, port 3001). It is configured by **provisioning files** mounted into
the container, so nothing is clicked by hand:

- a **data source** definition: type Postgres, address `catalog-db:5432`, database `iceberg`, user `grafana_live`;
- the **dashboard** definition (panels and their SQL queries), loaded as the home page.

Each panel is a SQL query on the `live` schema, re-run every **5 seconds**. Anonymous viewing is enabled for the
local demo (view only). The board shows events per second, each district's relative flow, the windows Flink flagged
and the incident alerts.

Grafana (live, operational) and Power BI (verified, analytical) answer different questions: "what is happening
now?" vs "what happened, and how good was the detection?".

### 5.13 Power BI

Power BI Desktop cannot reliably query the local Trino, so the last Spark step exports the marts and gold tables to
**Parquet files** in the repository folder `dashboards/data` (a bind mount into the Spark container). Power BI imports
those files. They are snapshots of verified results, regenerated after each run.

---

## Part 6. Dagster: running everything and proving it

### 6.1 Concepts

- **Asset**: a piece of data the pipeline produces (for example "the raw event archive for this run"), defined by a
  function that produces it **and checks it is correct**. Running that function is a **materialization**.
- **Dependencies**: each asset names the assets it needs; Dagster draws the graph and runs them in order.
- **Partition**: one copy of every asset per generated run (`run-72b96af88a342375`). A new dataset is a new
  partition; old ones are never touched.
- **Job**: a named selection of assets run together (`full_pipeline`, `batch_run`, ...).
- **Resource**: a shared client (Docker Compose, Kafka, MinIO, Flink REST, Trino REST).
- **Sensor**: a background check that starts work automatically (for example "a new run's archive is complete, start
  its batch job"). Ours are off by default.
- **Executor**: runs independent steps in parallel (up to 4), but at most **one Spark step at a time** (they would
  fight over memory and port 4040).

Why Dagster rather than Airflow: Dagster is lighter to run (one Python environment, no scheduler services or
containers of its own) and it models **data assets**, not just tasks, so lineage is built in: every table traces back
to the run and the steps that produced it.

### 6.2 The eleven assets and what each proves

| Asset | Does | Proves before continuing |
|---|---|---|
| `generator_run` | checks a generated run on disk | `_RUN_COMPLETE` exists, the generator's validation passed, line counts match the manifest |
| `raw_run_upload` | uploads reference files and the answer key | every object's size matches; `_RUN_COMPLETE` uploaded **last** |
| `kafka_delivery_streams` | publishes the run to Kafka (once) | before: topics, archiver and Flink jobs are running; after: Kafka gained exactly the expected counts |
| `raw_event_archive` | proves the archive is complete | archiver's committed offset is past the last message, file offset ranges cover everything with no gap; writes `archive-manifest.json` |
| `flink_district_outputs` | proves Flink processed everything | each job's committed offset reached the end of its input, in chain order; freezes output offsets |
| `batch_silver_gold` | runs the Spark batch build | `completed`, or `skipped` if the input is unchanged |
| `batch_validation` | runs an independent validator | re-derives the tables and reports `passed` |
| `streaming_iceberg_facts` | runs the bounded Spark load | Flink's results in Iceberg up to the frozen offsets |
| `alert_evaluation` | grades alerts against the answer key | must equal the Flink teammate's own evaluation script exactly |
| `dashboard_marts` | rebuilds the four Trino marts | row counts reported |
| `dashboard_exports` | writes Parquet for Power BI | rows per table reported |

### 6.3 "Proof, not waiting"

The key idea of the orchestration: **a finished command does not mean complete data**. The publisher can finish while
the archiver still holds 30 seconds of messages in memory; a Flink job can be "running" while it has not yet processed
the last records. Waiting a fixed time, or until topics stop growing, proves nothing (a slow producer can pause). So
Dagster uses facts that cannot lie:

- **Committed offsets**: a consumer group commits only after it has really processed messages (the archiver after the
  upload, Flink at a checkpoint). If the committed offset has reached the last published offset, everything was
  processed.
- **Offset ranges in file names**: the archive files must cover the published range with no gap.
- **Line-by-line comparison**: every archived line matches the original file.

### 6.4 State that makes re-running safe

These markers live **next to the data**, so they survive even if Dagster's history is deleted:

| Marker | Prevents |
|---|---|
| `water-raw/<run>/_RUN_COMPLETE` | Spark reading a half-uploaded run |
| `water-events/_orchestration/stream.json` (publication state, Kafka positions) | publishing a run twice |
| `water-events/_manifests/<run>/archive-manifest.json` | Spark reading an incomplete archive |
| Iceberg table `analytics.batch_manifest` (fingerprint + status) | rebuilding an unchanged run, or trusting a half-written one |

The general pattern: **write the completion marker last**. Anyone who sees the marker can trust everything before it.

A second full run therefore took 4.2 minutes: every step recognised finished work and did nothing twice.

### 6.5 The evaluation and the answer key

The answer key (`water-restricted`) is read by exactly one asset, after detection has finished. It matches each alert
to a planted incident (same district, timing within the incident), and reports: 12 of 12 detectable incidents found
(D006's constant leak is undetectable by any stream rule because its flow never changes), one false alarm (precision
0.94), detection delays, and 108 of 108 null-pressure faults flagged. It must produce exactly the same numbers as the
Flink teammate's original script.

---

## Part 7. Who talks to whom (the connection map)

| From | To | How | Address | For |
|---|---|---|---|---|
| Generator (tool container) | Kafka | Kafka protocol, producer | `kafka:9092` | publish events |
| Archiver | Kafka | consumer group `water-event-archiver` | `kafka:9092` | read both input topics |
| Archiver | MinIO | S3 API | `minio:9000` | write archive files |
| Flink TaskManager | Kafka | consumer groups `ewi-<job>`, producers | `kafka:9092` | read and write topics |
| Flink | its volume | file system | `/opt/flink/state` | checkpoints, savepoints |
| Spark (tool container) | MinIO | S3 API | `minio:9000` | read archive, read/write Iceberg files |
| Spark | Postgres | **JDBC** | `catalog-db:5432/iceberg` | Iceberg catalog pointers |
| Spark | Kafka | Kafka protocol, bounded read | `kafka:9092` | load Flink results |
| Trino | Postgres | **JDBC** | `catalog-db:5432/iceberg` | Iceberg catalog pointers |
| Trino | MinIO | S3 API | `minio:9000` | read/write Iceberg files |
| Live bridge | Kafka | consumer group `ewi-live-bridge` | `kafka:9092` | read Flink outputs |
| Live bridge | Postgres | Postgres protocol (Python driver) | `catalog-db:5432` | upsert into schema `live` |
| Grafana | Postgres | Postgres protocol, role `grafana_live` | `catalog-db:5432` | read schema `live` |
| Dagster (host) | Kafka | Kafka protocol | `localhost:29092` | topic admin, offsets, publishing checks |
| Dagster (host) | MinIO | S3 API | `localhost:9000` | upload, markers, manifests |
| Dagster (host) | Flink | REST API | `localhost:8081` | job status and checkpoint counts (jobs are submitted with the `flink run` command inside the JobManager container) |
| Dagster (host) | Trino | REST API | `localhost:8082` | run mart SQL |
| Dagster (host) | Docker | `docker compose` commands | local Docker | start tools (generator, Spark) |
| Power BI (host) | files | Parquet | `dashboards/data` | dashboard data |

Note that **JDBC is used only by Java engines (Spark, Trino) for the Iceberg catalog**. The live bridge (Python) and
Grafana use ordinary Postgres drivers to the same database.

---

## Part 8. One reading, end to end

Follow district D001's sensor reading for 2 January, 06:00 UTC: the minute a burst begins.

1. **Generated.** EPANET opens a leak (an emitter) in D001 at simulation minute 1,800. Inflow jumps from about 2 to
   about 6 litres per second; pressure falls slightly. The generator writes one JSON event with
   `event_id = run-72b9...:district:D001:001800`. The answer key records "burst in D001 from minute 1,800 to 1,920".
2. **Published.** The publisher sends it to `water.district-sensors.v1` and waits for Kafka's acknowledgement. Kafka
   stores it at some offset N in partition 0.
3. **Archived.** The archiver reads offset N, adds it to its buffer, and within 30 seconds writes a file named
   `part-0-<first>-<last>.ndjson.gz` whose range includes N, then commits its offset.
4. **Validated (Flink job 1).** The reading passes all nine checks; it is the first with this `event_id`, so it goes
   to the clean topic.
5. **Windowed (Flink job 2).** It falls into D001's 06:00–06:15 window. When the watermark passes 06:15, the window
   closes and one features row (average flow, minimum pressure, 15 readings) is written.
6. **Compared (Flink job 3).** D001's flow divided by the median of the other five is about 1.6: more than 10% above 1.
   One suspicious window. At 06:30 the second window is also suspicious, so an **R1 `open` alert** is emitted. When
   the burst ends and D001 returns to normal, a `closed` alert follows.
7. **Shown live.** The live bridge reads the alert and the relative-flow rows within a second and upserts them into
   `live.alerts` and `live.relative`. Grafana's next 5-second refresh shows D001's spike and the alert.
8. **Batch.** After the whole run is archived and proven complete, Spark reads the archive file containing offset N,
   cleans it, and adds its inlet volume to D001's 06:00–06:15 water-balance row. The meters in D001 did not rise, so
   the unmetered water in that interval rises sharply.
9. **Loaded.** The bounded load copies the alert into `water.streaming.fact_district_alert`.
10. **Joined.** Trino's `mart_incidents` pairs the open and closed alerts into one incident (delay 30 minutes);
    `mart_incident_balance` sees unmetered water up and metered consumption flat: `loss_like`. A real leak, not a
    demand surge.
11. **Graded.** The evaluation matches the incident to "burst in D001" in the answer key: detected, correct type,
    30-minute delay.
12. **Exported.** The marts go to Parquet; Power BI shows the incident on the overview and incidents pages.

---

## Part 9. What happens when something fails

| Failure | What happens | Why nothing is lost or duplicated |
|---|---|---|
| Publisher interrupted mid-run | Dagster detects a partial publication | verified resume: prove Kafka holds an exact prefix, send only the rest |
| Archiver crashes before committing | it restarts automatically and re-reads uncommitted messages | commit happens after upload; a possible duplicate file is detected by the offset-range check |
| Flink TaskManager crashes | jobs restart from the last checkpoint (≤ 30 s old) | state and offsets restored together; repeated output removed downstream by key |
| Spark batch crashes half-way | `batch_manifest` stays `processing` | nobody trusts the run; retry overwrites the same partition |
| Re-running the whole pipeline | every asset checks its marker | unchanged steps are verified and skipped (4.2 minutes total) |
| Live bridge restarts | it re-reads from its committed offset | upserts by natural key make re-reading harmless |
| Postgres lost | catalog pointers and live tables gone | live tables can be rebuilt from Kafka; catalog loss is serious (the files in MinIO remain, but tables would need re-registering): the reason production systems back up or replicate the catalog |
| MinIO lost | archive and tables gone | regenerate from the deterministic generator config (same seed → same data) |

---

## Part 10. Questions you may be asked

**Why simulate data instead of using real data?** Real network data never says where the leaks really were, so a
detector cannot be graded. Simulation with a hydraulic engine gives realistic, physically consistent data *and* an
answer key.

**Why Kafka?** It decouples producers from consumers, keeps data for replay, lets several independent consumers read
the same stream at their own pace, and gives exact positions (offsets) that make completeness provable.

**Why both streaming and batch?** They answer different questions. Streaming detects incidents within minutes from
district sensors. Batch waits for complete data and computes the exact water balance from 3.6 M meter readings. The
best result needs both: the meters (batch) decide whether a stream incident was a leak, a demand surge or a supply
failure, and only batch can see a leak that never changes (D006).

**Why does the batch read the archive and not Kafka directly?** Kafka keeps data for 7 days and is a transport; the
archive is a permanent, complete, provable copy. Reading only the archive also guarantees the batch sees exactly what
went through the stream.

**What is JDBC and why is it in your stack?** The standard Java API for talking to relational databases through a
driver and a URL. Our Iceberg catalog is a JDBC catalog: table pointers are rows in Postgres, and Spark and Trino (both
Java) connect to it via `jdbc:postgresql://catalog-db:5432/iceberg`. That shared catalog is why Trino sees Spark's
tables immediately.

**What exactly is stored in Postgres?** Two things in one database: the Iceberg catalog (only pointers to metadata
files; the table data is Parquet in MinIO) and the `live` schema (the real rows of the live board). It is one
container, `catalog-db`, not exposed outside Docker.

**Is "live Postgres" a container or a service?** Neither separately: it is the `live` schema inside the `catalog-db`
Postgres container. The **live bridge** is its own container that fills it; **Grafana** is its own container that
reads it.

**What makes an Iceberg write atomic?** New files are written first; the commit is a single compare-and-swap of the
table's metadata pointer in the catalog. Readers see the old or the new version, never a mix.

**How do you avoid duplicates if Kafka is at-least-once?** Every consumer either deduplicates by a stable key
(`event_id`, `alert_id`, district + window) or writes idempotently (upserts, partition overwrite). Repeats are expected
and harmless by design.

**What is a watermark?** Flink's running estimate of event-time progress: "events up to T have arrived". Windows close
when the watermark passes their end, so results depend on event time, not arrival time.

**Why compare a district with its neighbours?** All districts follow the same daily rhythm. Dividing by the median of
the others removes the rhythm, so a fixed threshold works at any hour. A fixed flow threshold would fire every morning.

**Why two consecutive windows?** To ignore single noisy windows. Cost: 15 extra minutes of delay.

**What was the false alarm?** An R4 level-shift alert in D004 on 21 January, the moment D005's 10-day loss ended: D005's
flow dropped, which lowered the peer median D004 is compared with, so D004 looked 3.4% higher than usual.

**Why can't the stream see D006's leak?** It exists from the first minute, so D006's flow never changes and its ratio
to the peers is constant. Only the batch water balance shows it: 4.96% of its water unmetered over 25 days.

**What is incremental processing here?** Each generated run is processed once. New runs are found by their archive
manifest; unchanged runs are skipped by fingerprint; each run writes only its own partition.

**Why Dagster over Airflow?** Simpler to run, and it models data assets with built-in lineage, which suits a pipeline
whose main concern is traceability and completeness.

**What would change in production?** Real meter feeds instead of the generator; more Kafka partitions (by district)
and more Flink/Spark workers; run IDs in Flink's output; separate, backed-up databases for the catalog and the live
store; secrets in a vault instead of a `.env` file; Grafana behind login.

---

## Glossary

- **Acks (`acks=all`)**: the producer waits until Kafka confirms the message is stored.
- **Asset (Dagster)**: a data product with a function that produces and checks it.
- **At-least-once**: every message is processed, some possibly twice.
- **Bind mount**: a host folder mapped into a container.
- **Broadcast join**: joining a small table by copying it to every task, avoiding a shuffle.
- **Catalog (Iceberg)**: maps table names to their current metadata file and performs atomic commits.
- **Checkpoint (Flink)**: periodic snapshot of job state and source offsets for recovery.
- **Committed offset**: the position a consumer group has confirmed processing up to.
- **Consumer group**: a named reader of Kafka topics with its own committed offsets.
- **Dead-letter topic**: where invalid records go, with the reason.
- **Event time / availability time**: when something happened / when it could first be seen.
- **Grain**: what one row represents (e.g. one district for 15 minutes).
- **Iceberg**: an open table format giving tables with atomic commits, snapshots and partitioning on object storage.
- **JDBC**: Java Database Connectivity, the standard Java way to connect to relational databases.
- **KRaft**: Kafka's built-in metadata management, replacing ZooKeeper.
- **Lakehouse**: tables with warehouse guarantees on top of data-lake storage.
- **Manifest (Iceberg)**: a file listing data files and their statistics.
- **Mart**: a table shaped for a dashboard.
- **NDJSON**: newline-delimited JSON, one record per line.
- **Object storage / bucket / key**: storage of whole files in named containers under names; S3 and MinIO.
- **Offset**: a message's position in a Kafka partition.
- **Parquet**: a compressed columnar file format.
- **Partition (Kafka)**: an ordered sub-log of a topic. **Partition (Iceberg)**: rows grouped by a column (here
  `run_id`) into separate files. **Partition (Dagster)**: one copy of each asset per run.
- **Partition overwrite**: replacing all rows of one partition in a single atomic write.
- **Savepoint**: a manual Flink checkpoint used to stop and resume jobs.
- **Schema (Postgres)**: a namespace of tables inside a database.
- **Shuffle**: moving rows between tasks so rows with the same key meet.
- **Snapshot (Iceberg)**: one committed version of a table.
- **Tumbling window**: fixed, non-overlapping time buckets.
- **Upsert**: insert, or update if a row with the same key exists.
- **Watermark**: Flink's estimate of how far event time has progressed.
