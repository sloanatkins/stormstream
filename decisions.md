# decisions.md

Running log of build decisions. Newest at the bottom.

## Session 1: Docker Compose skeleton

**Kafka in KRaft mode instead of Zookeeper.** Newer Kafka doesn't need Zookeeper, and one fewer container means a faster `docker compose up` and less to explain. Considered the Confluent images with Zookeeper but rejected them because they are heavier and the setup is older.

**Two Kafka listeners (INTERNAL and EXTERNAL).** Containers reach Kafka at `kafka:29092`, but the poller and Spark job run on my laptop at first, so they use `localhost:9092`. A single advertised address breaks one of those two cases, which is why there are two.

**Topic created by a one-shot `kafka-init` container.** This keeps topic creation in the repo instead of a manual command, and `--if-not-exists` makes reruns safe. 4 partitions, matching the ADD.

**Replication factor 1 everywhere.** One broker only, so anything higher would fail on startup. Fine for a portfolio demo and I'll say so in the README.

**Postgres schema left out on purpose.** Tables come in session 3 so I can test writes by hand before Spark touches them. The compose file already mounts `docker/postgres/init` for that.

## Session 2: NWS poller + Kafka producer

**Seen-store in SQLite instead of an in-memory dict.** A dict forgets everything on restart, so every active alert would be re-sent as new. SQLite ships with Python and keeps the state in one file. Considered Redis but rejected it because it is another container for one small table.

**Alerts that disappear from the feed are sent as `expired`.** NWS never announces that an alert ended, it just stops listing it. Comparing each poll to the last one is the only way to catch that.

**Revised alerts arrive as a new id.** NWS issues a revision as a brand new alert that points back to the old one in `references`. So a revision shows up as the old id expiring and a new id going active, and the two cancel out in the window counts. I could link them using `references` but decided it is not worth the extra code for this project.

**The store is only saved after the Kafka flush works.** If Kafka is down the cycle fails before the save, and the same changes are sent again next cycle. That makes delivery at-least-once, so the Postgres sink has to upsert on alert_id.

**Region comes from the first UGC code, not areaDesc.** areaDesc is free text like "Lake, FL; Volusia, FL". UGC codes start with the state, so the first two letters make a clean Kafka key.

**Bad alerts go to dead_letter.jsonl and only Actual alerts are kept.** A missing region means no Kafka key, so the alert is logged with the reason and skipped instead of crashing the loop. Test and Exercise alerts are filtered out.

**Still in the feed does not mean still active.** One alert had an expires time more than an hour in the past and NWS was still listing it. The dashboard should filter on expires_at too.

## Session 3: Postgres tables

**TIMESTAMPTZ instead of TIMESTAMP.** The ADD says TIMESTAMP, but my timestamps are all UTC with a +00:00 offset and plain TIMESTAMP silently drops the timezone. TIMESTAMPTZ keeps it, so there is no guessing which clock a time is on.

**CHECK constraints on severity, status and counts.** The data quality rules from the tech spec are enforced by the database as well as by the poller. I tested a made-up severity and a negative count by hand and both were rejected, so a bug in my Python can't put bad rows in.

**raw_alerts is one row per alert, upserted on alert_id.** An alert can arrive as active, then updated, then expired, and the row always holds the latest state. Kafka delivery is at-least-once, so the same message can show up twice, and the upsert makes that harmless.

**Added window_type to alert_window_agg and made it part of the primary key.** The ADD only has one aggregate table, but the spark job writes both 5 minute tumbling windows and 15 minute sliding windows. Without a type column the two would collide on the same start time. The key is also what lets a late alert overwrite a window's old count.

**pipeline_heartbeat has its own BIGSERIAL id.** Spark's batch_id restarts at 0 if the checkpoint folder is deleted, so using it as the primary key would collide with old rows. The dashboard just reads the newest row by written_at.

**Init SQL lives in docker/postgres/init, and I also applied it by hand.** Postgres only runs those files when the data volume is empty. A fresh clone gets the tables automatically, but my existing volume needed the manual run. I'll check the fresh clone case in session 9.

## Session 4: Spark reads Kafka and writes Postgres

**Dedicated Python 3.12 conda env.** The PySpark 4.0.1 docs say Python 3.9 or newer but never mention 3.13, which is what my base env had. A project env was safer, and it keeps requirements.txt honest.

**psycopg2 inside foreachBatch instead of Spark's JDBC writer.** The JDBC writer can only append or overwrite, there is no ON CONFLICT. A batch is a few rows, so pulling them to the driver and upserting is fine. At real volume I would write to a staging table and MERGE.

**Keep only the latest row per alert_id within a batch.** Postgres refuses a single upsert that touches the same row twice. I sort by Kafka offset, which is the true order because one alert always has the same key and so the same partition.

**Timestamps leave Spark as ISO text with +00:00.** PySpark hands python naive datetimes in the laptop's timezone, which would have shifted every time by 4 hours. Text with an explicit offset cannot be misread.

**Bad messages go to dead_letter_events, unique on (partition, offset).** Reading the same message twice cannot create duplicates. Tested by running the job twice, still 4 alerts and 4 dead letters.

**Postgres container moved to host port 15432.** My laptop already has Postgres on 5432 and the MacroLens container is on 5433, so the Spark job kept connecting to the wrong database. Containers still reach each other on 5432 inside the Docker network.

**Checkpoint folder plus a 10 second trigger.** The checkpoint is Spark's bookmark of which Kafka offsets are done. I tested it by stopping the job and restarting it: the next batch was number 2 and only handled the one new message, it did not re-read the old ones. startingOffsets=earliest only matters on the very first run now. The demo messages I sent by hand are still in the Kafka topic, so if I ever delete the checkpoint they will be read again, which is harmless because the writes are upserts.

## Session 5: windowing and watermarking

**Event time is ingested_at, not effective_at.** NWS effective times can be hours in the past or future, which would scatter alerts across windows that have nothing to do with when the pipeline saw them. ingested_at is when my poller picked the alert up, so windows line up with pipeline activity.

**Tumbling 5 minute and sliding 15 minute (5 minute slide) windows.** Both run in the same Spark app as separate queries. They write to one table, alert_window_agg, and a window_type column says which is which. I used update output mode and upsert on (window_type, window_start, region, severity), so a window row just gets overwritten as its count grows.

**alert_count means alert activity.** It counts new or changed alert events (status active or updated) in a window and leaves out expired ones. It does not mean how many alerts are active right now. The dashboard will get that number from raw_alerts instead.

**Watermark of 10 minutes.** Late events inside 10 minutes still get counted. Anything later is dropped from the windows but still lands in raw_alerts, so nothing is lost, it just does not change a closed window. I tested this with a late-1 message (counted) and a late-2 message (ignored for windows, kept in raw_alerts). A longer watermark would catch more late data but keeps state around longer.

**Duplicate deliveries.** Kafka is at-least-once, so the same message can show up twice. raw_alerts handles that with an upsert, but my window counts were counting events, so a replay inflated them. I resent late-1 a few times and the count went from 2 to 4. Fixed it with dropDuplicatesWithinWatermark on (alert_id, status, severity, expires_at). A real update changes severity or expiry so it still counts, and an exact replay gets dropped. After the fix the same window shows 2.

**Cost of the fix.** Adding the dedupe changed what the window checkpoints store, so I had to delete the tumbling and sliding checkpoints and truncate alert_window_agg once. raw_alerts kept its checkpoint.

## Session 6: heartbeat

**Heartbeat table is written by the Spark job, one row per batch.** Each row has the batch id and how many Kafka messages that batch read, written in the same transaction as the upserts, so a heartbeat only exists if the batch actually committed.

**Batch heartbeats alone were not enough.** Spark does not run a batch when no new messages arrive, so during a quiet stretch the table stopped updating. I tested it: one test message, then nothing, and the newest heartbeat just kept getting older (115 seconds, then 217) while the job was fine. A dashboard reading that would call a healthy pipeline dead whenever NWS is quiet.

**Fix: idle heartbeat from the main loop.** The program now wakes every 15 seconds and writes an idle row (batch_id -1, 0 events) if nothing has been written in the last 30 seconds. If any streaming query crashes, awaitAnyTermination raises and the loop ends, so a dead job stops heartbeating and the dashboard can show it as stale. The dashboard should treat a heartbeat older than about a minute as a problem.

**consumer_lag is left empty for now.** I did not want to guess at a number. Will revisit if there is time.

**Postgres retries.** I stopped the Postgres container for 20 seconds while the job was running and it crashed with connection refused. Every Postgres write now goes through one helper, pg_run, which opens a connection, runs the work in a single transaction, and retries up to 6 times with growing waits (2, 4, 6, 8, 10 seconds) if Postgres is unreachable. I repeated the 20 second outage: the job printed three retry lines and carried on once Postgres was back, no restart needed. If Postgres is down longer than about 30 seconds the job still crashes on purpose. That is safe, because the checkpoint resumes from the last committed batch and every write is an upsert. A crash also stops the heartbeat, which is how the dashboard finds out.

**Dead letters.** Two kinds of bad message reach the dead_letter_events table: text that is not JSON ("not valid JSON or no known fields") and valid JSON that breaks a rule, like a severity that is not allowed ("missing or invalid field"). I tested both. The raw text is kept along with the partition and offset, and none of them end up in raw_alerts. The table has a unique key on (partition, offset), so reading the same message twice does not create two rows.

**Batch latency.** A message took about 70 seconds from send to showing up in Postgres on my laptop, because three queries share one machine and each batch has to finish before the next starts. Fine for a portfolio demo, but the README should say end to end latency is around a minute locally and would be lower on a real cluster.
