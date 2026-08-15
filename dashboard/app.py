"""
StormStream Dashboard
=====================

Streamlit + Plotly dashboard that reads from PostgreSQL and shows:
1. Pipeline health (is the Spark job still alive)
2. Headline numbers: active alerts, severe or extreme, states affected
3. A US map colored by alert count or worst severity
4. A table of the active alerts
5. Alert activity over time (5 minute blocks or 15 minute rolling)
6. Dead letters (messages the Spark job rejected)

It only reads from Postgres, it never touches Kafka or Spark.

Run with:
    streamlit run dashboard/app.py

Dependencies:
- streamlit
- plotly
- psycopg2
- pandas
"""

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────
st.set_page_config(page_title="StormStream", layout="wide")

# ── Constants ─────────────────────────────────────────────────────────
SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]

# Darkest red for the worst, fading down to gray, so the colors mean the same thing everywhere
SEVERITY_COLORS = {
    "Extreme": "#7f1d1d",
    "Severe": "#dc2626",
    "Moderate": "#f97316",
    "Minor": "#eab308",
    "Unknown": "#9ca3af",
}
WHITE_TEXT = {"Extreme", "Severe"}  # dark enough backgrounds for white text, the rest get dark text
SEVERITY_RANK = {name: i for i, name in enumerate(SEVERITIES)}  # Extreme is 0, the worst
LOCAL_TZ = "America/New_York"

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
    "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}
# The region code is the first two letters of the NWS zone code, so it can also be
# a marine zone (PZ, AM, GM...) or a territory (GU, PR). Only real states get drawn on the map
US_STATES = set(STATE_NAMES)


# ── Helpers ───────────────────────────────────────────────────────────
def state_summary(active: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize the active alerts per state for the map.

    Returns:
    --------
    DataFrame with one row per state:
    - State (code like "FL")
    - Name (full name like "Florida")
    - Worst (worst severity among that state's alerts)
    - Alerts (how many active alerts)
    """

    in_states = active[active["region"].isin(US_STATES)]
    if in_states.empty:
        return pd.DataFrame(columns=["State", "Name", "Worst", "Alerts"])
    grouped = in_states.groupby("region").agg(
        Alerts=("alert_id", "count"),
        Worst=("severity", lambda s: min(s, key=SEVERITY_RANK.get)),
    )
    out = grouped.reset_index().rename(columns={"region": "State"})
    out["Name"] = out["State"].map(STATE_NAMES)
    return out


def query(sql: str) -> pd.DataFrame:
    """Run a SQL query against Postgres and return the result as a DataFrame."""

    # New connection every time, simple and fine for a small dashboard
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


def local_text(series: pd.Series, empty: str = "") -> pd.Series:
    """
    Format UTC timestamps as Eastern time text like "Oct 14, 3:12 PM".

    Missing values become the empty text.
    """

    ts = pd.to_datetime(series, utc=True).dt.tz_convert(LOCAL_TZ)
    return ts.dt.strftime("%b %-d, %-I:%M %p").fillna(empty)


def color_severity(value: str) -> str:
    """Return the CSS to color a table cell by its severity (empty if unknown)."""

    if value not in SEVERITY_COLORS:
        return ""
    text = "white" if value in WHITE_TEXT else "#1f2937"
    return f"background-color: {SEVERITY_COLORS[value]}; color: {text}; font-weight: 600"


# ── Data loaders ──────────────────────────────────────────────────────
@st.cache_data(ttl=10)  # re-query at most every 10 seconds
def load_active() -> pd.DataFrame:
    """Alerts that are still active right now, worst severity first."""

    # Status alone is not enough. An alert can pass its expires_at before the poller
    # notices it left the feed, so also check the expiry time
    return query("""
        SELECT alert_id, event_type, severity, region, effective_at, expires_at
        FROM raw_alerts
        WHERE status IN ('active', 'updated')
          AND (expires_at IS NULL OR expires_at > now())
          AND event_type <> 'Test Message'
        ORDER BY CASE severity WHEN 'Extreme' THEN 1 WHEN 'Severe' THEN 2
                 WHEN 'Moderate' THEN 3 WHEN 'Minor' THEN 4 ELSE 5 END,
                 effective_at DESC
    """)


@st.cache_data(ttl=5)
def load_heartbeat_age() -> pd.DataFrame:
    """One row with the seconds since the Spark job last wrote a heartbeat."""

    return query("SELECT extract(epoch FROM now() - max(written_at)) AS age FROM pipeline_heartbeat")


@st.cache_data(ttl=10)
def load_windows() -> pd.DataFrame:
    """Window counts per region and severity, oldest first."""

    return query("""
        SELECT window_type, window_start, region, severity, alert_count
        FROM alert_window_agg
        ORDER BY window_start
    """)


@st.cache_data(ttl=10)
def load_dead_letters() -> pd.DataFrame:
    """The 20 most recent rejected messages."""

    # The smoke test sends a throwaway message on purpose, leave those out
    return query("""
        SELECT failed_at, reason, left(raw, 80) AS raw, kafka_partition, kafka_offset
        FROM dead_letter_events
        WHERE raw IS NULL OR raw NOT LIKE 'smoke-%'
        ORDER BY failed_at DESC
        LIMIT 20
    """)


# ── Page header ───────────────────────────────────────────────────────
st.title("StormStream")
st.caption("US weather alerts, streamed from the NWS through Kafka and Spark into Postgres")

# ── Sidebar ───────────────────────────────────────────────────────────
# Filters live here, outside the fragment below (sidebar widgets can't be inside one).
# Nothing picked means no filter
active_all = load_active()
st.sidebar.header("Filters")
states = st.sidebar.multiselect("Region", sorted(active_all["region"].unique()))
severities = st.sidebar.multiselect("Severity", SEVERITIES)


# ── Live section ──────────────────────────────────────────────────────
@st.fragment(run_every="15s")  # this part re-runs by itself every 15 seconds
def live() -> None:
    """Everything on the page that refreshes on its own."""

    # ── Pipeline health ───────────────────────────────────────────────
    st.subheader("Pipeline health")
    age = load_heartbeat_age()["age"].iloc[0]
    if age is None or pd.isna(age):
        st.warning("No heartbeat yet. Has the Spark job ever run?")
    elif float(age) > 60:
        st.error(f"Spark job looks stuck: last heartbeat {int(age)} seconds ago")
    else:
        st.success(f"Spark job alive: last heartbeat {int(age)} seconds ago")

    # Apply the sidebar filters
    active = load_active()
    if states:
        active = active[active["region"].isin(states)]
    if severities:
        active = active[active["severity"].isin(severities)]

    # ── Headline numbers ──────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Active alerts", len(active))
    col2.metric("Severe or Extreme", int(active["severity"].isin(["Severe", "Extreme"]).sum()))
    col3.metric("States affected", active.loc[active["region"].isin(US_STATES), "region"].nunique())

    # ── Map ───────────────────────────────────────────────────────────
    st.subheader("Where alerts are active")
    summary = state_summary(active)
    color_by = st.radio("Color states by", ["Number of alerts", "Worst severity"], horizontal=True)
    if summary.empty:
        st.info("No active alerts in US states with these filters.")
    else:
        # The order here is the order of customdata[0], [1], [2] in the tooltip below
        hover = {"Name": True, "Worst": True, "Alerts": True, "State": False}
        if color_by == "Worst severity":
            fig = px.choropleth(
                summary, locations="State", locationmode="USA-states", scope="usa",
                color="Worst", color_discrete_map=SEVERITY_COLORS, category_orders={"Worst": SEVERITIES},
                hover_data=hover,
            )
            fig.update_layout(legend=dict(orientation="h", x=0, y=0, title_text=""))
        else:
            fig = px.choropleth(
                summary, locations="State", locationmode="USA-states", scope="usa",
                color="Alerts", color_continuous_scale=["#fde8e8", "#b91c1c"], hover_data=hover,
            )
            fig.update_layout(coloraxis_colorbar=dict(title="Alerts", thickness=12))
        fig.update_traces(hovertemplate=(
            "<b>%{customdata[0]}</b><br>Worst severity: %{customdata[1]}"
            "<br>Number of alerts: %{customdata[2]}<extra></extra>"
        ))
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    off_map = len(active) - int(active["region"].isin(US_STATES).sum())
    what = "the worst active severity" if color_by == "Worst severity" else "the number of active alerts"
    st.caption(f"Colored by {what} in each state. "
               f"{off_map} alerts for marine zones and territories are not on the map.")

    # ── Active alerts table ───────────────────────────────────────────
    st.subheader("Active alerts")
    view = pd.DataFrame({
        "Region": active["region"],
        "Event": active["event_type"],
        "Severity": active["severity"],
        "Effective": local_text(active["effective_at"]),
        "Expires": local_text(active["expires_at"], empty="No expiry"),
    })
    styled = view.style.map(color_severity, subset=["Severity"])
    st.dataframe(styled, width="stretch", hide_index=True, height=350)
    st.caption("Times are Eastern.")

    # ── Alert activity over time ──────────────────────────────────────
    st.subheader("Alert activity over time")
    # Friendly name -> (window_type in the table, minutes the window covers)
    window_choices = {
        "5-minute blocks": ("tumbling_5m", 5),
        "15-minute rolling (every 5 min)": ("sliding_15m", 15),
    }
    col_a, col_b = st.columns(2)
    choice = col_a.radio("Window", list(window_choices), horizontal=True)
    span = col_b.radio("Show", ["Last hour", "Last 6 hours", "Everything"], index=1, horizontal=True)
    window_type, minutes = window_choices[choice]

    windows = load_windows()
    windows = windows[windows["window_type"] == window_type].copy()
    if states:
        windows = windows[windows["region"].isin(states)]
    if severities:
        windows = windows[windows["severity"].isin(severities)]
    # Label each bar by when its window ends, like "the 15 minutes ending at 4:05 PM"
    windows["window_end"] = (pd.to_datetime(windows["window_start"], utc=True)
                             + pd.Timedelta(minutes=minutes)).dt.tz_convert(LOCAL_TZ)
    hours = {"Last hour": 1, "Last 6 hours": 6}.get(span)
    if hours:
        cutoff = pd.Timestamp.now(tz=LOCAL_TZ) - pd.Timedelta(hours=hours)
        windows = windows[windows["window_end"] >= cutoff]

    # The table has one row per state and severity, add them up so each color is one solid block
    windows = windows.groupby(["window_end", "severity"], as_index=False)["alert_count"].sum()

    if windows.empty:
        st.info("No window data in this time range.")
    else:
        fig = px.bar(
            windows, x="window_end", y="alert_count", color="severity",
            color_discrete_map=SEVERITY_COLORS, category_orders={"severity": SEVERITIES},
            labels={"window_end": "", "alert_count": "Alert events", "severity": ""},
        )
        fig.update_traces(
            width=4.2 * 60 * 1000,  # milliseconds, just under 5 minutes so bars do not touch
            # The severity as a bold title, then the time and the count with real labels
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%b %-d, %-I:%M %p}<br>Number of alerts: %{y}<extra></extra>",
        )
        fig.update_xaxes(tickformat="%b %-d<br>%-I:%M %p")
        # Pin the time axis so showing one severity does not stretch the bars across the whole chart
        pad = pd.Timedelta(minutes=5)
        x_lo = (windows["window_end"].min() - pad).strftime("%Y-%m-%d %H:%M:%S")
        x_hi = (windows["window_end"].max() + pad).strftime("%Y-%m-%d %H:%M:%S")
        fig.update_xaxes(range=[x_lo, x_hi])
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            # One click on a legend entry shows only that severity, click it again to show all.
            # Plotly's default is click = hide it, double click = show only it
            legend=dict(orientation="h", x=0, y=-0.2, itemclick="toggleothers", itemdoubleclick=False),
            uirevision="keep",  # keep what was clicked when the page refreshes itself
        )
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

    if minutes == 5:
        what = "Each bar is the 5 minutes ending at that time."
    else:
        what = "Each bar is the 15 minutes ending at that time, updated every 5 minutes, so neighboring bars overlap."
    st.caption(f"{what} Counts are new or changed alert events, in Eastern time. "
               "Gaps mean the pipeline was not running.")

    # ── Dead letters ──────────────────────────────────────────────────
    st.subheader("Dead letters")
    st.caption("Messages the Spark job rejected. The raw text is kept for debugging.")
    dead = load_dead_letters()
    dead["failed_at"] = local_text(dead["failed_at"])
    st.dataframe(dead, width="stretch", hide_index=True)


live()
