import json

import pytest
from pyspark.sql import SparkSession

from streaming.parsing import parse_alerts

GOOD = {
    "alert_id": "a1",
    "event_type": "Flood Watch",
    "severity": "Severe",
    "region": "FL",
    "effective_at": "2026-10-13T18:00:00+00:00",
    "expires_at": "2026-10-14T00:00:00+00:00",
    "status": "active",
    "ingested_at": "2026-10-13T18:01:00+00:00",
}


@pytest.fixture(scope="module")
def spark():
    # small local session, no kafka. starting it takes a few seconds
    session = (
        SparkSession.builder.master("local[1]")
        .appName("stormstream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def parse(spark, *messages):
    # each message is a dict (turned into json) or a plain string. returns rows in order
    rows = [(m if isinstance(m, str) else json.dumps(m), 0, i) for i, m in enumerate(messages)]
    df = spark.createDataFrame(rows, ["value", "partition", "offset"])
    return parse_alerts(df).orderBy("offset").collect()


def test_good_message_is_valid(spark):
    row = parse(spark, GOOD)[0]
    assert row.is_valid
    assert row.alert_id == "a1"
    assert row.region == "FL"


def test_missing_expiry_is_still_valid(spark):
    row = parse(spark, {**GOOD, "expires_at": None})[0]
    assert row.is_valid


def test_not_json_is_rejected_with_its_own_reason(spark):
    row = parse(spark, "hb-test-1")[0]
    assert not row.is_valid
    assert row.reason == "not valid JSON or no known fields"


def test_empty_object_is_rejected_with_its_own_reason(spark):
    row = parse(spark, "{}")[0]
    assert not row.is_valid
    assert row.reason == "not valid JSON or no known fields"


def test_missing_required_field_is_rejected(spark):
    bad = {k: v for k, v in GOOD.items() if k != "region"}
    row = parse(spark, bad)[0]
    assert not row.is_valid
    assert row.reason == "missing or invalid field"


def test_unknown_severity_word_is_rejected(spark):
    row = parse(spark, {**GOOD, "severity": "Catastrophic"})[0]
    assert not row.is_valid
    assert row.reason == "missing or invalid field"


def test_unknown_status_is_rejected(spark):
    row = parse(spark, {**GOOD, "status": "cancelled"})[0]
    assert not row.is_valid


def test_expired_status_is_valid(spark):
    row = parse(spark, {**GOOD, "status": "expired"})[0]
    assert row.is_valid


def test_bad_timestamp_is_rejected(spark):
    row = parse(spark, {**GOOD, "ingested_at": "yesterday-ish"})[0]
    assert not row.is_valid


def test_batch_keeps_good_and_bad_apart(spark):
    rows = parse(spark, GOOD, "garbage", {**GOOD, "alert_id": "a2"})
    assert [r.is_valid for r in rows] == [True, False, True]
    assert [r.offset for r in rows] == [0, 1, 2]
