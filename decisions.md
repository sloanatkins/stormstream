# decisions.md

Running log of build decisions. Newest at the bottom.

## Session 1: Docker Compose skeleton

**Kafka in KRaft mode instead of Zookeeper.** Newer Kafka doesn't need Zookeeper, and one fewer container means a faster `docker compose up` and less to explain. Considered the Confluent images with Zookeeper but rejected them because they are heavier and the setup is older.

**Two Kafka listeners (INTERNAL and EXTERNAL).** Containers reach Kafka at `kafka:29092`, but the poller and Spark job run on my laptop at first, so they use `localhost:9092`. A single advertised address breaks one of those two cases, which is why there are two.

**Topic created by a one-shot `kafka-init` container.** This keeps topic creation in the repo instead of a manual command, and `--if-not-exists` makes reruns safe. 4 partitions, matching the ADD.

**Replication factor 1 everywhere.** One broker only, so anything higher would fail on startup. Fine for a portfolio demo and I'll say so in the README.

**Postgres schema left out on purpose.** Tables come in session 3 so I can test writes by hand before Spark touches them. The compose file already mounts `docker/postgres/init` for that.
