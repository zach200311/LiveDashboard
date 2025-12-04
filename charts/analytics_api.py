
import os
import sys
import traceback
from datetime import datetime, timedelta
from collections import defaultdict

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import mysql.connector
import certifi

# ----------------- CONFIG -----------------
API_PORT = int(os.getenv("API_PORT", "5001"))

DB_HOST = os.getenv("DB_HOST") or os.getenv(
    "MYSQL_HOST", "richardsonsql.mysql.database.azure.com"
)
DB_PORT = int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("DB_USER") or os.getenv("MYSQL_USER", "utdsql")
DB_PASS = (
    os.getenv("DB_PASS")
    or os.getenv("DB_PASSWORD")
    or os.getenv("MYSQL_PASSWORD")
    or "Capstone2025!"
)
DB_NAME = os.getenv("DB_NAME") or os.getenv("MYSQL_DATABASE", "kafka")

# --------- RUSH TABLES ONLY ---------
FACT_TABLE = "kafka_pipeline_rush"
FACT_COL_SPEED = "peakspeed"
FACT_COL_PMG = "pmgid"
FACT_COL_TS = "ts"
FACT_COL_VEH = "vehiclecount"
FACT_COL_LOCATION = "location"
FACT_COL_DIRECTION = "direction"
FACT_COL_RECORD = "record_id"

LAT_VIEW = "kafka_latency_ms_rush"
LAT_COL_RECORD = "record_id"
LAT_COL_STAGE_SENSOR_INGEST = "ms_sensor_to_ingest"
LAT_COL_STAGE_SENSOR_SQL = "ms_sensor_to_sql"
LAT_COL_STAGE_SQL_EMIT = "ms_sql_to_emit"
LAT_COL_STAGE_EMIT_RENDER = "ms_emit_to_render"
LAT_COL_STAGE_SENSOR_RENDER = "ms_sensor_to_render"  # end-to-end

# Lookback windows
TREND_HOURS = int(os.getenv("TREND_HOURS", "6"))
HIST_MINUTES = int(os.getenv("HIST_MINUTES", "30"))
SCATTER_MINUTES = int(os.getenv("SCATTER_MINUTES", "30"))
DAILY_DAYS = int(os.getenv("DAILY_DAYS", "7"))
KPI_MINUTES_DEFAULT = int(os.getenv("KPI_MINUTES", "15"))

# Latency outlier cap (anything above this ms is ignored in medians/pcts)
MAX_LAT_MS = int(os.getenv("MAX_LAT_MS", "30000"))  # 30 seconds


# ----------------- APP + DB HELPERS -----------------
app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)


def db():
    """Open a new DB connection."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ssl_ca=certifi.where(),
        ssl_disabled=False,
    )


def q_rows(sql, params=()):
    """Run a query and return all rows."""
    conn = db()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def log_exc(tag: str):
    print(f"\n=== API ERROR [{tag}] ===", file=sys.stderr)
    traceback.print_exc()


def pct(values, p: float):
    """Compute percentile in Python (0–1)."""
    if not values:
        return None
    data = sorted(values)
    k = (len(data) - 1) * p
    i = int(k)
    j = min(i + 1, len(data) - 1)
    if i == j:
        return float(data[i])
    frac = k - i
    return float(data[i] * (1 - frac) + data[j] * frac)


def minutes_ago(m: int) -> datetime:
    return datetime.utcnow() - timedelta(minutes=m)


def hours_ago(h: int) -> datetime:
    return datetime.utcnow() - timedelta(hours=h)


def days_ago(d: int) -> datetime:
    return datetime.utcnow() - timedelta(days=d)


# ----------------- ROUTES -----------------
@app.get("/")
def root():
    return send_from_directory(".", "index.html")


@app.get("/api/health")
def health():
    try:
        conn = db()
        conn.close()
        return jsonify(
            {
                "db": DB_NAME,
                "fact_table": FACT_TABLE,
                "latency_view": LAT_VIEW,
                "ok": True,
            }
        )
    except Exception as e:
        log_exc("health")
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------- KPI ----------
@app.get("/api/kpi")
def kpi():
    """
    KPI cards (RUSH-only).

    ?mins=N   -> use last N minutes for the window (default KPI_MINUTES_DEFAULT)

    Vehicles / detections:
      - events_total                  = COUNT(*)
      - events_per_minute_total       = events_total / window_mins
      - events_per_minute_per_sensor  = events_total / (window_mins * sensors_active)
      - sensors_active                = COUNT(DISTINCT pmgid)
    """
    try:
        mins_str = request.args.get("mins")
        try:
            window_mins = int(mins_str) if mins_str is not None else KPI_MINUTES_DEFAULT
        except ValueError:
            window_mins = KPI_MINUTES_DEFAULT

        # Latest speed + timestamp
        latest_row = q_rows(
            f"""
            SELECT {FACT_COL_SPEED}, {FACT_COL_TS}
            FROM {FACT_TABLE}
            ORDER BY {FACT_COL_TS} DESC
            LIMIT 1
            """
        )
        latest_speed = None
        latest_ts = None
        data_age_seconds = None
        if latest_row:
            latest_speed = (
                float(latest_row[0][0]) if latest_row[0][0] is not None else None
            )
            latest_ts_val = latest_row[0][1]
            if isinstance(latest_ts_val, datetime):
                if latest_ts_val.tzinfo is not None:
                    latest_ts_utc = latest_ts_val.astimezone(tz=None).replace(
                        tzinfo=None
                    )
                else:
                    latest_ts_utc = latest_ts_val
                data_age_seconds = (datetime.utcnow() - latest_ts_utc).total_seconds()
                latest_ts = latest_ts_utc

        since_ts = minutes_ago(window_mins)

        # Avg speed + total events + active sensors
        agg_row = q_rows(
            f"""
            SELECT
              AVG({FACT_COL_SPEED})          AS avg_speed,
              COUNT(*)                       AS events_total,
              COUNT(DISTINCT {FACT_COL_PMG}) AS sensors_active
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            """,
            (since_ts,),
        )

        avg_speed = None
        events_total = 0
        sensors_active = 0
        if agg_row:
            if agg_row[0][0] is not None:
                avg_speed = round(float(agg_row[0][0]), 2)
            if agg_row[0][1] is not None:
                events_total = int(agg_row[0][1])
            if agg_row[0][2] is not None:
                sensors_active = int(agg_row[0][2])

        events_per_minute_total = (
            float(events_total) / float(window_mins) if window_mins > 0 else None
        )
        events_per_minute_per_sensor = (
            float(events_total)
            / float(window_mins * sensors_active)
            if window_mins > 0 and sensors_active > 0
            else None
        )

        # Latency for the window (end-to-end = ms_sensor_to_render)
        lat_rows = q_rows(
            f"""
            SELECT l.{LAT_COL_STAGE_SENSOR_RENDER}
            FROM {FACT_TABLE} p
            JOIN {LAT_VIEW}   l
              ON p.{FACT_COL_RECORD} = l.{LAT_COL_RECORD}
            WHERE p.{FACT_COL_TS} >= %s
            """,
            (since_ts,),
        )
        lat_values = [
            float(r[0])
            for r in lat_rows
            if r[0] is not None and float(r[0]) <= MAX_LAT_MS
        ]
        median_latency = pct(lat_values, 0.5) if lat_values else None

        return jsonify(
            {
                "latest_speed": latest_speed,
                "latest_ts": latest_ts.isoformat() + "Z" if latest_ts else None,
                "data_age_seconds": data_age_seconds,
                "avg_speed_window": window_mins,
                "avg_speed": avg_speed,
                "events_total": events_total,
                "events_per_minute_total": events_per_minute_total,
                "events_per_minute_per_sensor": events_per_minute_per_sensor,
                "sensors_active": sensors_active,
                # Backwards-compatible keys
                "events": events_total,
                "vehicles": events_total,
                "events_per_minute": events_per_minute_total,
                "median_pipeline_time": median_latency,
            }
        )
    except Exception as e:
        log_exc("kpi")
        return jsonify({"error": str(e)}), 500


# ---------- Peak speed trend ----------
@app.get("/api/series/peak_speed_trend")
def peak_speed_trend():
    """Average peakspeed per minute over last TREND_HOURS hours (RUSH only)."""
    try:
        since_ts = hours_ago(TREND_HOURS)
        rows = q_rows(
            f"""
            SELECT DATE_FORMAT({FACT_COL_TS}, '%%Y-%%m-%%d %%H:%%i:00') AS bucket,
                   AVG({FACT_COL_SPEED}) AS avg_speed
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (since_ts,),
        )
        cats = [r[0] for r in rows]
        data = [round(float(r[1] or 0), 2) for r in rows]
        return jsonify(
            {
                "categories": cats,
                "series": [{"name": "Avg Peak Speed (mph)", "data": data}],
            }
        )
    except Exception as e:
        log_exc("peak_speed_trend")
        return jsonify({"categories": [], "series": [], "error": str(e)}), 500


# ---------- Throughput (events per minute) ----------
@app.get("/api/agg/throughput_total")
def throughput_total():
    """Count of RUSH events per minute over last TREND_HOURS hours."""
    try:
        since_ts = hours_ago(TREND_HOURS)
        rows = q_rows(
            f"""
            SELECT DATE_FORMAT({FACT_COL_TS}, '%%Y-%%m-%%d %%H:%%i:00') AS bucket,
                   COUNT(*) AS c
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (since_ts,),
        )
        cats = [r[0] for r in rows]
        data = [int(r[1]) for r in rows]
        return jsonify(
            {
                "categories": cats,
                "series": [{"name": "Events / minute", "data": data}],
            }
        )
    except Exception as e:
        log_exc("throughput_total")
        return jsonify({"categories": [], "series": [], "error": str(e)}), 500


# ---------- Pipeline latency percentiles vs time ----------
@app.get("/api/agg/pipeline_latency")
def pipeline_latency():
    """
    Latency p50/p90/p99 per minute from kafka_latency_ms_rush (RUSH only),
    using ms_sensor_to_render as end-to-end latency.
    """
    try:
        since_ts = hours_ago(TREND_HOURS)
        rows = q_rows(
            f"""
            SELECT DATE_FORMAT(p.{FACT_COL_TS}, '%%Y-%%m-%%d %%H:%%i:00') AS bucket,
                   l.{LAT_COL_STAGE_SENSOR_RENDER} AS ms_end
            FROM {FACT_TABLE} p
            JOIN {LAT_VIEW}   l
              ON p.{FACT_COL_RECORD} = l.{LAT_COL_RECORD}
            WHERE p.{FACT_COL_TS} >= %s
            ORDER BY bucket ASC
            """,
            (since_ts,),
        )

        buckets = defaultdict(list)
        for bucket, ms in rows:
            if ms is None:
                continue
            ms_val = float(ms)
            if ms_val > MAX_LAT_MS:
                continue
            buckets[bucket].append(ms_val)

        cats = sorted(buckets.keys())
        p50 = [pct(buckets[b], 0.5) for b in cats]
        p90 = [pct(buckets[b], 0.9) for b in cats]
        p99 = [pct(buckets[b], 0.99) for b in cats]

        return jsonify(
            {
                "categories": cats,
                "p50": p50,
                "p90": p90,
                "p99": p99,
            }
        )
    except Exception as e:
        log_exc("pipeline_latency")
        return jsonify(
            {"categories": [], "p50": [], "p90": [], "p99": [], "error": str(e)},
            500,
        )


# ---------- Speed histogram ----------
@app.get("/api/agg/speed_histogram")
def speed_histogram():
    """
    Histogram of peakspeed for the last N minutes.

    ?bin_width=N (mph)
    ?mins=M      (minutes, default HIST_MINUTES)
    """
    try:
        bin_width = request.args.get("bin_width", "2")
        mins_str = request.args.get("mins")
        try:
            bw = max(1, int(bin_width))
        except ValueError:
            bw = 2
        try:
            window_mins = int(mins_str) if mins_str is not None else HIST_MINUTES
        except ValueError:
            window_mins = HIST_MINUTES

        since_ts = minutes_ago(window_mins)
        rows = q_rows(
            f"""
            SELECT FLOOR({FACT_COL_SPEED} / %s) * %s AS bin_start,
                   COUNT(*) AS c
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY bin_start
            ORDER BY bin_start
            """,
            (bw, bw, since_ts),
        )

        centers = []
        counts = []
        for bin_start, c in rows:
            if bin_start is None:
                continue
            centers.append(int(bin_start) + bw / 2.0)
            counts.append(int(c))

        return jsonify(
            {
                "centers": centers,
                "counts": counts,
                "bin_width": bw,
                "window_mins": window_mins,
            }
        )
    except Exception as e:
        log_exc("speed_histogram")
        return jsonify(
            {"centers": [], "counts": [], "bin_width": 0, "error": str(e)}, 500
        )


# ---------- Daily speed percentiles ----------
@app.get("/api/agg/speed_percentiles_daily")
def speed_percentiles_daily():
    """
    Daily distribution of peakspeed over the last DAILY_DAYS days.
    Percentiles: 50th, 85th, 95th.
    """
    try:
        since_ts = days_ago(DAILY_DAYS)
        rows = q_rows(
            f"""
            SELECT DATE({FACT_COL_TS}) AS d, {FACT_COL_SPEED}
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            ORDER BY d ASC
            """,
            (since_ts,),
        )
        by_day = defaultdict(list)
        for d, speed in rows:
            if speed is not None:
                by_day[str(d)].append(float(speed))

        cats = sorted(by_day.keys())
        p50 = [pct(by_day[d], 0.5) for d in cats]
        p85 = [pct(by_day[d], 0.85) for d in cats]
        p95 = [pct(by_day[d], 0.95) for d in cats]

        series = [
            {"name": "p50", "data": p50},
            {"name": "p85", "data": p85},
            {"name": "p95", "data": p95},
        ]
        return jsonify({"categories": cats, "series": series})
    except Exception as e:
        log_exc("speed_percentiles_daily")
        return jsonify({"categories": [], "series": [], "error": str(e)}), 500


# ---------- PMGID volume ----------
@app.get("/api/agg/pmgid_volume")
def pmgid_volume():
    """Top PMGIDs by event count over last TREND_HOURS hours."""
    try:
        since_ts = hours_ago(TREND_HOURS)
        rows = q_rows(
            f"""
            SELECT {FACT_COL_PMG},
                   COALESCE(MAX({FACT_COL_LOCATION}), 'Unknown') AS loc,
                   COUNT(*) AS c
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY {FACT_COL_PMG}
            ORDER BY c DESC
            LIMIT 25
            """,
            (since_ts,),
        )
        cats = []
        counts = []
        for pmg, loc, c in rows:
            pmg_str = str(pmg)
            loc_str = loc or "Unknown"
            label = f"{loc_str} ({pmg_str})" if loc_str != "Unknown" else pmg_str
            cats.append(label)
            counts.append(int(c))

        series = [{"name": "Events", "data": counts}]
        return jsonify({"categories": cats, "series": series})
    except Exception as e:
        log_exc("pmgid_volume")
        return jsonify({"categories": [], "series": [], "error": str(e)}), 500


# ---------- Behaviour profile: per-sensor speed vs detections/min ----------
@app.get("/api/agg/speed_vs_vehicle_scatter")
def speed_vs_vehicle_scatter():
    """
    Behaviour profile.

    Each point is ONE SENSOR (PMGID) in the selected window:
      x = average peak speed at that sensor (mph)
      y = detections per minute at that sensor

    ?mins=M -> window (default SCATTER_MINUTES)
    """
    try:
        mins_str = request.args.get("mins")
        try:
            window_mins = int(mins_str) if mins_str is not None else SCATTER_MINUTES
        except ValueError:
            window_mins = SCATTER_MINUTES

        since_ts = minutes_ago(window_mins)
        rows = q_rows(
            f"""
            SELECT {FACT_COL_PMG},
                   AVG({FACT_COL_SPEED}) AS avg_speed,
                   COUNT(*)              AS events
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY {FACT_COL_PMG}
            """,
            (since_ts,),
        )

        pts = []
        for pmg, avg_speed, events in rows:
            if avg_speed is None or events is None:
                continue
            per_min = float(events) / float(window_mins) if window_mins > 0 else 0.0
            pts.append([float(avg_speed), per_min])

        return jsonify(
            {
                "window_mins": window_mins,
                "series": [{"name": "Sensor behaviour", "data": pts}],
            }
        )
    except Exception as e:
        log_exc("speed_vs_vehicle_scatter")
        return jsonify({"series": [], "error": str(e)}), 500


# ---------- Latency breakdown by stage ----------
@app.get("/api/latency_breakdown_stages")
def latency_breakdown_stages():
    """
    Median latency per pipeline stage from kafka_latency_ms_rush.
    Uses ALL rows in the view (RUSH only), with outliers capped by MAX_LAT_MS.
    """
    try:
        rows = q_rows(
            f"""
            SELECT
              {LAT_COL_STAGE_SENSOR_INGEST},
              {LAT_COL_STAGE_SENSOR_SQL},
              {LAT_COL_STAGE_SQL_EMIT},
              {LAT_COL_STAGE_EMIT_RENDER},
              {LAT_COL_STAGE_SENSOR_RENDER}
            FROM {LAT_VIEW}
            """
        )
        s_ingest = []
        s_sql = []
        s_sql_emit = []
        s_emit_render = []
        s_end_to_end = []
        for (
            ms_ingest,
            ms_sql,
            ms_sql_emit,
            ms_emit_render,
            ms_render,
        ) in rows:
            if ms_ingest is not None and float(ms_ingest) <= MAX_LAT_MS:
                s_ingest.append(float(ms_ingest))
            if ms_sql is not None and float(ms_sql) <= MAX_LAT_MS:
                s_sql.append(float(ms_sql))
            if ms_sql_emit is not None and float(ms_sql_emit) <= MAX_LAT_MS:
                s_sql_emit.append(float(ms_sql_emit))
            if ms_emit_render is not None and float(ms_emit_render) <= MAX_LAT_MS:
                s_emit_render.append(float(ms_emit_render))
            if ms_render is not None and float(ms_render) <= MAX_LAT_MS:
                s_end_to_end.append(float(ms_render))

        stages = [
            "Sensor → Ingest",
            "Sensor → SQL",
            "SQL → Emit",
            "Emit → Render",
            "Sensor → Render (End to End)",
        ]
        medians = [
            pct(s_ingest, 0.5),
            pct(s_sql, 0.5),
            pct(s_sql_emit, 0.5),
            pct(s_emit_render, 0.5),
            pct(s_end_to_end, 0.5),
        ]
        return jsonify({"stages": stages, "medians": medians})
    except Exception as e:
        log_exc("latency_breakdown_stages")
        return jsonify({"stages": [], "medians": [], "error": str(e)}), 500


# ---------- Location × direction snapshot ----------
@app.get("/api/location_direction_snapshot")
def location_direction_snapshot():
    """
    Snapshot of volume & avg speed by (location, direction) over a configurable window.

    ?mins=M -> window minutes (default KPI_MINUTES_DEFAULT * 4)
    """
    try:
        mins_str = request.args.get("mins")
        try:
            window_mins = int(mins_str) if mins_str is not None else KPI_MINUTES_DEFAULT * 4
        except ValueError:
            window_mins = KPI_MINUTES_DEFAULT * 4

        since_ts = minutes_ago(window_mins)
        rows = q_rows(
            f"""
            SELECT CONCAT({FACT_COL_LOCATION}, ' / ', {FACT_COL_DIRECTION}) AS locdir,
                   COUNT(*) AS c,
                   AVG({FACT_COL_SPEED}) AS avg_speed
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY locdir
            ORDER BY c DESC
            LIMIT 25
            """,
            (since_ts,),
        )
        cats = []
        vol = []
        avg = []
        for locdir, c, avg_speed in rows:
            cats.append(locdir or "Unknown")
            vol.append(int(c))
            avg.append(float(avg_speed or 0.0))

        return jsonify(
            {
                "window_mins": window_mins,
                "categories": cats,
                "volume": vol,
                "avg_speed": avg,
            }
        )
    except Exception as e:
        log_exc("location_direction_snapshot")
        return jsonify(
            {"categories": [], "volume": [], "avg_speed": [], "error": str(e)}, 500
        )


# ---------- Hourly profile ----------
@app.get("/api/hourly_profile")
def hourly_profile():
    """
    Volume & avg speed by hour-of-day over last DAILY_DAYS days.
    """
    try:
        since_ts = days_ago(DAILY_DAYS)
        rows = q_rows(
            f"""
            SELECT HOUR({FACT_COL_TS}) AS h,
                   COUNT(*)            AS c,
                   AVG({FACT_COL_SPEED}) AS avg_speed
            FROM {FACT_TABLE}
            WHERE {FACT_COL_TS} >= %s
            GROUP BY h
            ORDER BY h
            """,
            (since_ts,),
        )
        hours = []
        volume = []
        avg_speed = []
        for h, c, avg_s in rows:
            hours.append(int(h))
            volume.append(int(c))
            avg_speed.append(float(avg_s or 0.0))

        return jsonify({"hours": hours, "volume": volume, "avg_speed": avg_speed})
    except Exception as e:
        log_exc("hourly_profile")
        return jsonify(
            {"hours": [], "volume": [], "avg_speed": [], "error": str(e)}, 500
        )

# ---------- Heatmap: speed × time × vehicle count ----------
@app.get("/api/heatmap")
def heatmap():
    """
    Heatmap of speed vs time.
    Uses CURDATE() + TIME column to build valid datetime for window filtering.
    """
    try:
        rows = q_rows(
            """
            SELECT 
                DATE_FORMAT(CONCAT(CURDATE(), ' ', timestamp), '%%H:%%i') AS minute_bucket,
                FLOOR(peakspeed / 5) * 5 AS speed_bin,
                COUNT(*) AS c
            FROM raw_data_kafka
            WHERE CONCAT(CURDATE(), ' ', timestamp) >= NOW() - INTERVAL 60 MINUTE
            GROUP BY minute_bucket, speed_bin
            ORDER BY minute_bucket ASC, speed_bin ASC
            """
        )

        if not rows:
            return jsonify({"times": [], "speeds": [], "values": []})

        # Build axes
        times = sorted({row[0] for row in rows})
        speeds = sorted({int(row[1]) for row in rows})

        t_index = {t: i for i, t in enumerate(times)}
        s_index = {s: i for i, s in enumerate(speeds)}

        values = []
        for minute_bucket, speed_bin, count in rows:
            values.append([
                t_index[minute_bucket],
                s_index[int(speed_bin)],
                int(count)
            ])

        return jsonify({
            "times": times,
            "speeds": speeds,
            "values": values
        })

    except Exception as e:
        log_exc("heatmap")
        return jsonify({"times": [], "speeds": [], "values": [], "error": str(e)})

# ---------- Bubble map: location points ----------
@app.get("/api/location_bubble")
def api_location_bubble():
    try:
        rows = q_rows(
            """
            SELECT location, COUNT(*) AS c
            FROM raw_data_kafka
            WHERE CONCAT(CURDATE(), ' ', timestamp) >= NOW() - INTERVAL 60 MINUTE
            GROUP BY location
            """
        )

        points = []
        for loc, count in rows:
            if not loc:
                continue

            try:
                lat_str, lon_str = loc.split(",")
                lat = float(lat_str.strip())
                lon = float(lon_str.strip())

                points.append({
                    "x": lon,        # longitude is X
                    "y": lat,        # latitude is Y
                    "z": int(count)  # bubble size
                })

            except Exception:
                continue

        return jsonify({"points": points})

    except Exception as e:
        log_exc("location_bubble")
        return jsonify({"points": [], "error": str(e)})


# ---------- Direction distribution ----------
@app.get("/api/direction_distribution")
def api_direction_distribution():
    try:
        rows = q_rows(
            """
            SELECT direction, COUNT(*) AS c
            FROM raw_data_kafka
            WHERE CONCAT(CURDATE(), ' ', timestamp) >= NOW() - INTERVAL 60 MINUTE
            GROUP BY direction
            """
        )

        data = []
        for direction, count in rows:
            # direction may be INT but Highcharts needs string labels
            label = str(direction)
            data.append({"name": label, "y": int(count)})

        return jsonify({"data": data})

    except Exception as e:
        log_exc("direction_distribution")
        return jsonify({"data": [], "error": str(e)})


@app.get("/api/live_speed")
def api_live_speed():
    try:
        rows = q_rows(
            """
            SELECT ts, peakspeed
            FROM kafka_pipeline_rush
            ORDER BY ts DESC
            LIMIT 50
            """
        )

        points = []

        for ts, speed in rows:
            if speed is None or ts is None:
                continue

            # ts comes from MySQL TIME column → Python timedelta object
            # Convert HH:MM:SS(.mmm) → seconds since midnight
            total_seconds = ts.seconds + ts.microseconds / 1_000_000

            # Convert to milliseconds for Highcharts
            ts_ms = int(total_seconds * 1000)

            points.append([ts_ms, float(speed)])

        # Make sure points are in chronological order
        points.reverse()

        return jsonify({"points": points})

    except Exception as e:
        log_exc("live_speed")
        return jsonify({"points": []})


if __name__ == "__main__":
    print(f"Starting analytics API on 0.0.0.0:{API_PORT}")
    app.run(host="0.0.0.0", port=API_PORT, debug=True)
