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
