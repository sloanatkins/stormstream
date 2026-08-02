"""StormStream dashboard. It only reads from Postgres, it never touches Kafka or Spark."""
import os

import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="StormStream", layout="wide")

SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]


def query(sql):
    # new connection every time, simple and fine for a small dashboard
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "15432")),
        dbname=os.getenv("POSTGRES_DB", "stormstream"),
        user=os.getenv("POSTGRES_USER", "stormstream"),
        password=os.getenv("POSTGRES_PASSWORD", "stormstream"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


@st.cache_data(ttl=10)  # re-query at most every 10 seconds
def load_active():
    # status alone is not enough. an alert can pass its expires_at before the poller
    # notices it left the feed, so also check the expiry time
    return query("""
        SELECT alert_id, event_type, severity, region, effective_at, expires_at
        FROM raw_alerts
        WHERE status IN ('active', 'updated')
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY CASE severity WHEN 'Extreme' THEN 1 WHEN 'Severe' THEN 2
                 WHEN 'Moderate' THEN 3 WHEN 'Minor' THEN 4 ELSE 5 END,
                 effective_at DESC
    """)


@st.cache_data(ttl=5)
def load_heartbeat_age():
    return query("SELECT extract(epoch FROM now() - max(written_at)) AS age FROM pipeline_heartbeat")


@st.cache_data(ttl=10)
def load_windows():
    return query("""
        SELECT window_type, window_start, severity, alert_count
        FROM alert_window_agg
        ORDER BY window_start
    """)


@st.cache_data(ttl=10)
def load_dead_letters():
    return query("""
        SELECT failed_at, reason, left(raw, 80) AS raw, kafka_partition, kafka_offset
        FROM dead_letter_events
        ORDER BY failed_at DESC
        LIMIT 20
    """)


st.title("StormStream")
st.caption("US weather alerts, streamed from the NWS through Kafka and Spark into Postgres")

# filters live in the sidebar, outside the fragment below (sidebar widgets can't be inside one).
# nothing picked means no filter
active_all = load_active()
st.sidebar.header("Filters")
states = st.sidebar.multiselect("State", sorted(active_all["region"].unique()))
severities = st.sidebar.multiselect("Severity", SEVERITIES)


@st.fragment(run_every="15s")  # this part re-runs by itself every 15 seconds
def live():
    st.subheader("Pipeline health")
    age = load_heartbeat_age()["age"].iloc[0]
    if age is None or pd.isna(age):
        st.warning("No heartbeat yet. Has the Spark job ever run?")
    elif float(age) > 60:
        st.error(f"Spark job looks stuck: last heartbeat {int(age)} seconds ago")
    else:
        st.success(f"Spark job alive: last heartbeat {int(age)} seconds ago")

    active = load_active()
    if states:
        active = active[active["region"].isin(states)]
    if severities:
        active = active[active["severity"].isin(severities)]

    col1, col2, col3 = st.columns(3)
    col1.metric("Active alerts", len(active))
    col2.metric("Severe or Extreme", int(active["severity"].isin(["Severe", "Extreme"]).sum()))
    col3.metric("States affected", active["region"].nunique())

    st.subheader("Active alerts")
    st.dataframe(active, use_container_width=True, hide_index=True, height=350)

    st.subheader("Alert activity by window")
    kind = st.radio("Window type", ["tumbling_5m", "sliding_15m"], horizontal=True)
    windows = load_windows()
    windows = windows[windows["window_type"] == kind]
    if windows.empty:
        st.info("No window data yet.")
    else:
        # one bar per window, split by severity
        chart = windows.pivot_table(index="window_start", columns="severity",
                                    values="alert_count", aggfunc="sum").fillna(0)
        st.bar_chart(chart)
    st.caption("Counts are new or changed alert events per window. Sliding windows overlap, "
               "so the same event shows up in up to three of them.")

    st.subheader("Dead letters")
    st.caption("Messages the Spark job rejected. The raw text is kept for debugging.")
    st.dataframe(load_dead_letters(), use_container_width=True, hide_index=True)


live()
