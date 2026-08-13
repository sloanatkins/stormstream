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

## Session 7: dashboard

**Streamlit only reads Postgres.** It never touches Kafka or Spark, so the dashboard keeps working (with stale data) if the stream is down, and I can say exactly how fresh it is from the heartbeat.

**Active means not expired, not just status active.** An alert can pass its expires_at before the poller notices it left the NWS feed, so the dashboard filters on both status and expires_at. Of the 452 alerts the poller saw on a fresh start, 337 passed that filter.

**Health panel reads the heartbeat table.** Green if the newest heartbeat is under 60 seconds old, red if not. I tested it by stopping the Spark job: the box went red at 74 seconds and went back to green after a restart. Because of the idle heartbeat from Session 6, a quiet pipeline stays green and a dead one turns red.

**The first poll makes one huge bar in the window chart.** Windows use ingestion time, and on a fresh start all 452 current alerts arrive at once, so about 450 events land in a single window. That is the first snapshot, not 450 new alerts in five minutes. After that the poller only sends changes and the bars are small. For the demo I will record after the poller has been running for a while.

**Auto refresh every 15 seconds** with a Streamlit fragment, and a 10 second cache on each query so the page is not hammering Postgres. Filters sit in the sidebar because widgets there cannot live inside a fragment.

## Session 8: tests

**What is tested.** The diff logic (new, unchanged, updated, expired, duplicate ids), the NWS parser (good alert, every kind of bad alert, UTC conversion) and the Spark validity rules (valid, not JSON, bad severity or status, missing field, bad timestamp). None of the poller tests need Kafka or Docker, so they run in a fraction of a second. The Spark tests start a small local session and take about 15 seconds.

**Tests have to be able to fail.** I broke the fingerprint on purpose by taking severity out of it, and exactly the two tests that should catch it failed. Then I restored the file.

**Moved parse_alerts into its own file.** Importing stream_alerts.py starts the whole streaming job, so a test could not import it. parsing.py holds the validity logic and stream_alerts.py imports it.

**The tests found a real bug.** On Spark 4, casting a malformed string to a timestamp throws an error instead of returning null, so my comment saying a bad timestamp becomes null was wrong. A message with a garbage timestamp would have crashed the job, and since the checkpoint cannot move past it, every restart would crash again. Fixed with try_cast, which returns null, so the message goes to dead letters. The poller checks timestamps itself, so this could only have been hit by a hand-sent or foreign message, but catching those is what the dead letter table is for.

## Session 9: CI and clean clone

**CI runs the tests on every push.** A GitHub Actions workflow sets up Python 3.12 and Java 21 (the Spark tests need Java), installs requirements.txt from scratch and runs pytest. First run was green in about a minute with all 30 tests. Because it installs on a blank machine, it also proves requirements.txt is complete.

**CI only covers the unit tests.** It does not start Kafka or Postgres. The infrastructure is covered by the smoke test script and by running the full pipeline by hand, which is a known gap I would close with a docker compose job in CI if I kept building on this.

**Clean clone check.** I removed the stack and its volumes, cloned the repo from GitHub into a new folder, copied .env.example to .env and ran docker compose up. The smoke test passed 9 of 9 and the init SQL created all four tables on its own on the empty volume. Then I ran the poller, the Spark job and the dashboard from the clone: 354 active alerts across 31 states, and git status stayed empty afterward, so nothing the pipeline writes is missing from .gitignore.

## Dashboard polish (went back to the Session 7 dashboard after Session 9)

**One severity color scale everywhere.** Extreme is dark red, fading through red and orange to yellow and gray for Unknown. The first version let the chart pick colors alphabetically, so Extreme came out blue and Moderate red, which was misleading.

**Region, not state.** The region is the first two letters of the NWS zone code, so it also includes marine zones (PZ, PK, AM) and territories like Guam. The table says Region, the map only draws the 50 states and DC with a note about how many alerts are left off, and the States affected number only counts real states.

**Unknown severity is real.** I checked the live NWS feed and it sends Unknown itself for Air Quality Alerts and Hydrologic Outlooks. NWS test messages are filtered out of the dashboard.

**Map has two views.** Worst active severity per state, or number of alerts. The worst severity view is almost all red because NWS grades Watches as Severe, so the count view is the default.

**Chart bars are labeled by when the window ends** ("the 15 minutes ending at 4:05 PM") with plain names instead of tumbling_5m and sliding_15m, and a last hour / 6 hours / everything switch.

**The startup spike is a known limitation.** On a fresh start the first poll reports every active alert as new, so one window shows about 470 events. Fixing it properly means marking the first poll as a snapshot all the way through the pipeline, which is more work than it is worth here. It only happens once, and the last hour view hides it after an hour.

## Dashboard polish round 2 (after the pipeline went down for a day)

**What happened.** My Mac logged me out on Sunday morning and everything stopped: Docker, the poller, and the Spark job. When I restarted on Monday the dashboard had a gap of about a day in the chart and a second huge spike on the right. The gap is real, the pipeline was not running. The spike is the same startup spike as before: the poller's local store was a day old, so it reported all ~365 active alerts as new at once. I did not change either one. I added a line to the chart caption saying gaps mean the pipeline was not running, and put dates on the time axis, because with only times on it I could not tell how long the gap was.

**Map.** Hover now shows the state name, the worst severity, and the number of alerts. Plotly draws lakes as white shapes on top of the states by default, which showed up as holes in Utah (Great Salt Lake) and Florida (Lake Okeechobee). I turned lakes off. Alaska and Hawaii are drawn as small insets under the Southwest. That is how this map projection works, so I left it.

**Filters.** The sidebar region and severity filters changed the metrics, map and table but not the chart, because the chart query did not select the region column. It does now.

**Pinned dependencies.** requirements.txt had no versions, so a fresh clone could install newer Streamlit, pandas or plotly and behave differently. I pinned everything to the versions I tested with. I first ran pip list in the wrong conda environment (base instead of stormstream) and got Streamlit 1.45 instead of 1.66, so the lesson is to check which environment the terminal is in before copying versions.

**Not doing: running it 24/7.** The pipeline only runs while my laptop is on. Running it all the time would mean a small cloud server (Kafka plus Spark needs about 4 GB of RAM, so roughly $30 a month) and a day or two of work on containers and secrets. For a portfolio project I decided the README, screenshots and a demo video cover what matters, since a dead link looks worse than no link. I may deploy it to EC2 later as an extra.

**Not doing: dashboard tests.** app.py runs Streamlit code when imported, so testing its helper functions would mean splitting them into a separate file. That is a refactor for little gain here, and the 30 pipeline tests already cover the parts that carry the logic.
