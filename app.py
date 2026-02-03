import os
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import pymysql
from datetime import date, datetime, time, timedelta

def clean(val):
    if isinstance(val, (datetime, date, time)):
        return str(val)
    if isinstance(val, timedelta):
        # Convert timedelta to HH:MM:SS
        total_seconds = int(val.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return val

def make_json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return str(obj)
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj

def fix_timedelta(row):
    """Convert any timedelta fields in a row dict to 'HH:MM:SS' strings."""
    if not row:
        return row
    fixed = {}
    for k, v in row.items():
        if isinstance(v, timedelta):
            total = int(v.total_seconds())
            h = total // 3600
            m = (total % 3600) // 60
            s = total % 60
            fixed[k] = f"{h:02d}:{m:02d}:{s:02d}"
        else:
            fixed[k] = v
    return fixed

def fix_timedelta_row(row):
    """In-place version used for lists of rows."""
    if not row:
        return row
    for k, v in row.items():
        if isinstance(v, timedelta):
            total_seconds = int(v.total_seconds())
            h = total_seconds // 3600
            m = (total_seconds % 3600) // 60
            s = total_seconds % 60
            row[k] = f"{h:02d}:{m:02d}:{s:02d}"
    return row

def clean_value(v):
    try:
        v = str(v).strip()
        if v in ["NA", "na", "NaN", "-", "--", "", "null", "None"]:
            return None
        return float(v)
    except Exception:
        return None

def calculate_aqi(pollutants):
    """
    pollutants: dict with keys:
        'PM2.5', 'PM10', 'SO2', 'NO2', 'O3', 'CO', 'NH3'
    units: µg/m³ except CO (mg/m³)
    """
    breakpoints = {
        "PM2.5": [
            (0, 30, 0, 50),
            (31, 60, 51, 100),
            (61, 90, 101, 200),
            (91, 120, 201, 300),
            (121, 250, 301, 400),
            (251, 500, 401, 500),
        ],
        "PM10": [
            (0, 50, 0, 50),
            (51, 100, 51, 100),
            (101, 250, 101, 200),
            (251, 350, 201, 300),
            (351, 430, 301, 400),
            (431, 600, 401, 500),
        ],
        "SO2": [
            (0, 40, 0, 50),
            (41, 80, 51, 100),
            (81, 380, 101, 200),
            (381, 800, 201, 300),
            (801, 1600, 301, 400),
            (1601, 2620, 401, 500),
        ],
        "NO2": [
            (0, 40, 0, 50),
            (41, 80, 51, 100),
            (81, 180, 101, 200),
            (181, 280, 201, 300),
            (281, 400, 301, 400),
            (401, 800, 401, 500),
        ],
        "O3": [
            (0, 50, 0, 50),
            (51, 100, 51, 100),
            (101, 168, 101, 200),
            (169, 208, 201, 300),
            (209, 748, 301, 400),
            (749, 1000, 401, 500),
        ],
        "CO": [
            (0, 1, 0, 50),
            (1.1, 2, 51, 100),
            (2.1, 10, 101, 200),
            (10.1, 17, 201, 300),
            (17.1, 34, 301, 400),
            (34.1, 50, 401, 500),
        ],
        "NH3": [
            (0, 200, 0, 50),
            (201, 400, 51, 100),
            (401, 800, 101, 200),
            (801, 1200, 201, 300),
            (1201, 1800, 301, 400),
            (1801, 2500, 401, 500),
        ],
    }

    def compute_iaqi(Cp, bp_list):
        if Cp is None:
            return None
        for BP_Lo, BP_Hi, I_Lo, I_Hi in bp_list:
            if BP_Lo <= Cp <= BP_Hi:
                return ((I_Hi - I_Lo) / (BP_Hi - BP_Lo)) * (Cp - BP_Lo) + I_Lo
        return None

    iaqis = {}
    for p, bp in breakpoints.items():
        iaqis[p] = compute_iaqi(pollutants.get(p), bp)

    overall_aqi = max([v for v in iaqis.values() if v is not None], default=None)
    return int(overall_aqi) if overall_aqi is not None else None, iaqis

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=env_path)
    print("Loaded .env from:", env_path)
except Exception as e:
    print("WARNING: dotenv not loaded:", e)

PORT = int(os.getenv("PORT", 5001))
HOST = os.getenv("HOST", "0.0.0.0")
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY", "")
INDIA_DATA_API_KEY = os.getenv("INDIA_DATA_API_KEY", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Harshit@5993")
DB_NAME = os.getenv("DB_NAME", "harshit")


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_latest_pollutant_reading_for_station(station_display_name: str | None):
    """
    EXACT station match on location_name.
    If no row for that station → fallback to latest overall.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if station_display_name:
                cur.execute(
                    """
                    SELECT *
                    FROM pollutant_readings
                    WHERE location_name = %s
                    ORDER BY reading_date DESC, reading_time DESC, record_id DESC
                    LIMIT 1
                    """,
                    (station_display_name,),
                )
                row = cur.fetchone()
                if row:
                    return fix_timedelta(row)

            # fallback: latest overall
            cur.execute(
                """
                SELECT *
                FROM pollutant_readings
                ORDER BY reading_date DESC, reading_time DESC, record_id DESC
                LIMIT 1
                """
            )
            return fix_timedelta(cur.fetchone())
    finally:
        conn.close()

def get_latest_meteorological_reading_for_station(station_display_name: str | None):
    """
    EXACT station match on station_name in meteorological_data.
    If no row for that station → fallback to latest overall.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if station_display_name:
                cur.execute(
                    """
                    SELECT *
                    FROM meteorological_data
                    WHERE station_name = %s
                    ORDER BY record_date DESC, record_time DESC, record_id DESC
                    LIMIT 1
                    """,
                    (station_display_name,),
                )
                row = cur.fetchone()
                if row:
                    return fix_timedelta(row)

            # fallback: latest any station
            cur.execute(
                """
                SELECT *
                FROM meteorological_data
                ORDER BY record_date DESC, record_time DESC, record_id DESC
                LIMIT 1
                """
            )
            return fix_timedelta(cur.fetchone())
    finally:
        conn.close()

def fetch_openweather(lat: float, lon: float):
    """
    Fetch 5-day/3h forecast from OpenWeather for a given lat/lon.
    We only use the first element (next forecast) for dashboard.
    """
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "units": "metric",
        "appid": WEATHERAPI_KEY,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_pollutant_data(pollutant_id: str):
    API_URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
    params = {
        "api-key": INDIA_DATA_API_KEY,
        "format": "json",
        "filters[state]": "Delhi",
        "filters[pollutant_id]": pollutant_id,
        "limit": 1000,
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_or_create_station_id(conn, station_name, latitude=None, longitude=None):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT station_id FROM stations WHERE name = %s", (station_name,)
        )
        row = cur.fetchone()
        if row:
            return row["station_id"]

        cur.execute(
            """
            INSERT INTO stations (name, latitude, longitude)
            VALUES (%s, %s, %s)
            """,
            (station_name, latitude, longitude),
        )
        conn.commit()
        return cur.lastrowid

def create_user(first_name, middle_name, last_name, user_name, age):
    print(f"➡️ Attempting to save user: {user_name}")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            # Check if username exists
            cur.execute(
                "SELECT user_id FROM users WHERE user_name = %s", (user_name,)
            )
            if cur.fetchone():
                return {"status": "error", "message": "Username already exists"}

            # Insert required fields
            sql = """
                INSERT INTO users
                (first_name, middle_name, last_name, user_name, age)
                VALUES (%s, %s, %s, %s, %s)
            """

            cur.execute(
                sql,
                (
                    first_name,
                    middle_name,
                    last_name,
                    user_name,
                    age
                )
            )

            conn.commit()
            return {"status": "success", "user_id": cur.lastrowid}

    except Exception as e:
        print("DB Error:", e)
        return {"status": "error", "message": str(e)}

    finally:
        conn.close()

def save_pollutant_records_to_db(records):
    """
    Saves ONLY ONE latest pollutant reading per station per sync.
    Groups all pollutants of a station, computes AQI, inserts one row.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            grouped = {}

            for rec in records:
                station = (
                    rec.get("station")
                    or rec.get("station_name")
                    or rec.get("location")
                    or rec.get("city")
                )
                if not station:
                    continue

                station = station.strip().replace(" ,", ",").replace("  ", " ")

                if station not in grouped:
                    grouped[station] = {
                        "PM2.5": None,
                        "PM10": None,
                        "SO2": None,
                        "NO2": None,
                        "OZONE": None,
                        "CO": None,
                        "NH3": None,
                        "latitude": clean_value(rec.get("latitude")),
                        "longitude": clean_value(rec.get("longitude")),
                        "timestamp": None,
                    }

                # pollutant ID normalize
                pid = str(rec.get("pollutant_id", "")).upper()
                if pid in grouped[station]:
                    grouped[station][pid] = clean_value(rec.get("avg_value"))

                # timestamp normalize
                ts_raw = (
                    rec.get("last_update")
                    or rec.get("date")
                    or rec.get("timestamp")
                )
                ts = None
                if ts_raw:
                    try:
                        ts = datetime.fromisoformat(ts_raw)
                    except Exception:
                        try:
                            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            ts = None

                if ts:
                    old = grouped[station]["timestamp"]
                    if old is None or ts > old:
                        grouped[station]["timestamp"] = ts

            # insert final rows per station
            for station_name, pollutants in grouped.items():
                now = datetime.now()
                reading_date = now.strftime("%Y-%m-%d")
                reading_time = now.strftime("%H:%M:%S")

                lat = pollutants.get("latitude")
                lon = pollutants.get("longitude")

                station_id = get_or_create_station_id(
                    conn, station_name, latitude=lat, longitude=lon
                )

                pollutant_data = {
                    "PM2.5": pollutants.get("PM2.5"),
                    "PM10": pollutants.get("PM10"),
                    "SO2": pollutants.get("SO2"),
                    "NO2": pollutants.get("NO2"),
                    "O3": pollutants.get("OZONE"),
                    "CO": pollutants.get("CO"),
                    "NH3": pollutants.get("NH3"),
                }

                aqi, _ = calculate_aqi(pollutant_data)

                cur.execute(
                    """
                    INSERT INTO pollutant_readings
                    (station_id, location_name,
                     pm25_ug_m3, so2_ug_m3, no2_ug_m3,
                     PM10, CO, OZONE, NH3,
                     reading_date, reading_time, aqi)
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s)
                    """,
                    (
                        station_id,
                        station_name,
                        pollutants.get("PM2.5"),
                        pollutants.get("SO2"),
                        pollutants.get("NO2"),
                        pollutants.get("PM10"),
                        pollutants.get("CO"),
                        pollutants.get("OZONE"),
                        pollutants.get("NH3"),
                        reading_date,
                        reading_time,
                        aqi,
                    ),
                )

        conn.commit()
        print(f"✅ Saved {len(grouped)} station rows into pollutant_readings")
    finally:
        conn.close()

def save_openweather_to_db(weather_json, station_name: str):
    """
    Save a single weather snapshot for one station into meteorological_data.
    location_name will be the station_name (so front-end can query by station).
    """
    if not weather_json:
        return

    try:
        entry = weather_json["list"][0]

        temperature = entry["main"]["temp"]
        feels_like = entry["main"].get("feels_like")
        pressure = entry["main"].get("pressure")
        grnd_level = entry["main"].get("grnd_level")
        humidity = entry["main"]["humidity"]
        wind_speed = entry["wind"]["speed"]
        wind_deg = entry["wind"].get("deg")
        wind_gust = entry["wind"].get("gust")
        clouds_all = entry.get("clouds", {}).get("all")
        visibility = entry.get("visibility", 0) / 1000.0
        pop = entry.get("pop")
        rain_3h = entry.get("rain", {}).get("3h")

        weather_main = entry["weather"][0]["main"]
        weather_desc = entry["weather"][0]["description"]

        city_block = weather_json.get("city", {})
        sunrise_ts = city_block.get("sunrise")
        sunset_ts = city_block.get("sunset")

        sunrise = (
            datetime.fromtimestamp(sunrise_ts).strftime("%H:%M:%S")
            if sunrise_ts else None
        )
        sunset = (
            datetime.fromtimestamp(sunset_ts).strftime("%H:%M:%S")
            if sunset_ts else None
        )

        record_date = datetime.now().strftime("%Y-%m-%d")
        record_time = datetime.now().strftime("%H:%M:%S")

        # ----------------------------------------------------
        # ⭐ FIX: Resolve station_id correctly
        # ----------------------------------------------------
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT station_id FROM stations WHERE name = %s",
                (station_name,)
            )
            row = cur.fetchone()
            if row:
                station_id = row["station_id"]
            else:
                cur.execute(
                    "INSERT INTO stations (name) VALUES (%s)",
                    (station_name,)
                )
                conn.commit()
                station_id = cur.lastrowid

            # NOW safe to insert
            cur.execute(
                """
                INSERT INTO meteorological_data(
                    temperature_c, feels_like_c, pressure_hpa, grnd_level_hpa,
                    humidity_percent, wind_kph, wind_deg, wind_gust,
                    visibility_km, clouds_percent,
                    precipitation_prob, rain_3h,
                    condition_main, condition_text,
                    sunrise, sunset, record_date, record_time,
                    station_id, station_name
                )
                VALUES (%s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s)
                """,
                (
                    temperature,
                    feels_like,
                    pressure,
                    grnd_level,
                    humidity,
                    wind_speed,
                    wind_deg,
                    wind_gust,
                    visibility,
                    clouds_all,
                    pop,
                    rain_3h,
                    weather_main,
                    weather_desc,
                    sunrise,
                    sunset,
                    record_date,
                    record_time,
                    station_id,
                    station_name,
                ),
            )
            conn.commit()

        conn.close()
    except Exception as e:
        print(f"OpenWeather Save Error for {station_name}:", e)

def sync_external_data():
    """
    Called by fetch.py every hour (no Flask request context).
    - Pollutant sync: same as before
    - Weather sync: now per station, using stations.latitude/longitude
    """

    print("🔄 sync_external_data(): starting external API sync")

    # 1) Pollutant sync (India API)
    try:
        pollutant_ids = ["PM2.5", "SO2", "NO2", "OZONE", "CO", "NH3", "PM10"]
        all_records = []

        for pid in pollutant_ids:
            resp = fetch_pollutant_data(pid)
            recs = resp.get("records", [])

            for r in recs:
                r["pollutant_id"] = pid  # normalize pollutant id

            all_records.extend(recs)

        if all_records:
            save_pollutant_records_to_db(all_records)
        else:
            print("⚠️ India API returned NO pollutant data")

    except Exception as e:
        print("⚠️ India API sync error:", e)

    # 2) Weather sync per station
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, latitude, longitude
                FROM stations
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """
            )
            stations = cur.fetchall()

        conn.close()

        print(f"🌤  Fetching weather for {len(stations)} stations")

        for s in stations:
            name = s["name"]
            lat = s["latitude"]
            lon = s["longitude"]

            if lat is None or lon is None:
                print(f"Skipping station {name}: missing lat/lon")
                continue

            try:
                wjson = fetch_openweather(lat, lon)
                save_openweather_to_db(wjson, name)
            except Exception as e:
                print(f"⚠️ Weather sync error for {name}:", e)

    except Exception as e:
        print("⚠️ Station weather sync error:", e)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
@app.route("/api/combined_data", methods=["GET"])
def combined_data():
    """
    Used by frontend dashboard.
    - Reads latest pollutant row for the requested station
    - Reads latest meteorological row for the same station
    """
    station = request.args.get("station")
    print(f"/api/combined_data called for station: {station}")

    db_pollutants = get_latest_pollutant_reading_for_station(station)
    db_meteo = get_latest_meteorological_reading_for_station(station)

    return jsonify(
        make_json_safe(
            {
                "location": station,
                "pollutant_data": db_pollutants,
                "meteorological_data_db": db_meteo,
            }
        )
    )

@app.post("/api/register_user")
def register_user_endpoint():
    data = request.json or {}

    result = create_user(
        data.get("first_name"),
        data.get("middle_name"),
        data.get("last_name"),
        data.get("user_name"),
        data.get("age")
    )

    status_code = 200 if result["status"] == "success" else 400
    return jsonify(result), status_code

@app.route("/api/insert_pollutant", methods=["POST"])
def insert_pollutant():
    data = request.get_json()
    pollutant_data = {
        "PM2.5": data.get("pm25_ug_m3"),
        "PM10": data.get("PM10"),
        "SO2": data.get("so2_ug_m3"),
        "NO2": data.get("no2_ug_m3"),
        "O3": data.get("OZONE"),
        "CO": data.get("CO"),
        "NH3": data.get("NH3"),
    }
    aqi, _ = calculate_aqi(pollutant_data)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pollutant_readings
                (station_id, location_name,
                 pm25_ug_m3, so2_ug_m3, no2_ug_m3,
                 PM10, CO, OZONE, NH3,
                 reading_date, reading_time, aqi)
                VALUES (%s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s)
                """,
                (
                    data.get("station_id"),
                    data.get("location_name"),
                    data.get("pm25_ug_m3"),
                    data.get("so2_ug_m3"),
                    data.get("no2_ug_m3"),
                    data.get("PM10"),
                    data.get("CO"),
                    data.get("OZONE"),
                    data.get("NH3"),
                    data.get("reading_date"),
                    data.get("reading_time"),
                    aqi,
                ),
            )
            conn.commit()
    finally:
        conn.close()
    return jsonify({"status": "ok", "aqi": aqi}), 201

@app.route("/api/insert_meteorological", methods=["POST"])
def insert_meteorological():
    data = request.get_json()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meteorological_data
                (temperature_c, feels_like_c, pressure_hpa, grnd_level_hpa,
                 humidity_percent, wind_kph, wind_deg, wind_gust, visibility_km, clouds_percent,
                 precipitation_prob, rain_3h, condition_main, condition_text,
                 sunrise, sunset, record_date, record_time,station_id , station_name)
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s, %s ,%s ,%s)
                """,
                (
                    data.get("temperature_c"),
                    data.get("feels_like_c"),
                    data.get("pressure_hpa"),
                    data.get("grnd_level_hpa"),
                    data.get("humidity_percent"),
                    data.get("wind_kph"),
                    data.get("wind_deg"),
                    data.get("wind_gust"),
                    data.get("visibility_km"),
                    data.get("clouds_percent"),
                    data.get("precipitation_prob"),
                    data.get("rain_3h"),
                    data.get("condition_main"),
                    data.get("condition_text"),
                    data.get("sunrise"),
                    data.get("sunset"),
                    data.get("record_date"),
                    data.get("record_time"),
                    data.get("station_id"),
                    data.get("station_name"),
                ),
            )
            conn.commit()
    finally:
        conn.close()
    return jsonify({"status": "ok"}), 201

@app.route('/api/station', methods=['GET'])
def get_all_stations():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT station_id, name FROM stations")
        rows = cursor.fetchall()
        return jsonify(rows), 200
    except Exception as e:
        print("❌ ERROR:", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/pollutant_trend")
def pollutant_trend():
    station = request.args.get("station")
    pollutant = request.args.get("pollutant")  # example: pm25_ug_m3

    VALID = {
        "pm25_ug_m3",
        "so2_ug_m3",
        "no2_ug_m3",
        "OZONE",
        "CO",
        "NH3",
        "PM10",
    }

    if pollutant not in VALID:
        return jsonify({"error": "Invalid pollutant"}), 400

    query = f"""
        SELECT reading_date, reading_time, {pollutant}
        FROM pollutant_readings
        WHERE location_name = %s
        ORDER BY reading_date DESC, reading_time DESC
        LIMIT 48
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (station,))
            rows = cur.fetchall()
            fixed_rows = [fix_timedelta_row(r) for r in rows]
            fixed_rows.reverse()  # oldest → latest
        return jsonify(fixed_rows)
    finally:
        conn.close()

@app.route("/api/temp_trend", methods=["GET"])
def temp_trend():
    """
    Returns exactly 12 hourly points (across all stations combined).
    Missing hours are filled with None.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    DATE_FORMAT(record_time, '%H') AS hour_slot,
                    AVG(temperature_c) AS temp_avg
                FROM meteorological_data
                WHERE CONCAT(record_date, ' ', record_time) >= NOW() - INTERVAL 12 HOUR
                GROUP BY hour_slot
                ORDER BY hour_slot DESC;
                """
            )
            rows = cur.fetchall()

        data_map = {
            r["hour_slot"]: float(r["temp_avg"]) if r["temp_avg"] is not None else None
            for r in rows
        }

        result = []
        for i in range(11, -1, -1):  # oldest → latest
            hour = (datetime.now() - timedelta(hours=i)).strftime("%H")
            result.append(
                {
                    "record_time": f"{hour}:00",
                    "temperature_c": data_map.get(hour),
                }
            )

        return jsonify(result)
    finally:
        conn.close()

@app.post("/api/login_user")
def login_user():
    data = request.json or {}
    user_name = data.get("user_name")

    conn = get_db_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:

            # Fetch user info
            cur.execute("""
                SELECT user_id, first_name, middle_name, last_name,
                       age
                FROM users
                WHERE user_name = %s
            """, (user_name,))
            
            user = cur.fetchone()

            if not user:
                return jsonify({"status": "error", "message": "Invalid username"}), 400

            # Store user ID in session
            session["user_id"] = user["user_id"]

            return jsonify({
                "status": "success",
                "user_id": user["user_id"],
                "first_name": user["first_name"],
                "middle_name": user["middle_name"],
                "last_name": user["last_name"],
                "age": user["age"],
            }), 200

    except Exception as e:
        print("Login Error:", e)
        return jsonify({"status": "error", "message": str(e)}), 400

    finally:
        conn.close()

@app.post("/api/register_user")
def register_user():
    data = request.json or {}

    first = data.get("first_name")
    last = data.get("last_name")
    mid = data.get("middle_name")
    uname = data.get("user_name")
    age = data.get("age")

    conn = get_db_connection()
    try:
        cur = conn.cursor()  # NO dictionary cursor here
        cur.execute("""
            INSERT INTO users (first_name, last_name, middle_name, user_name, age)
            VALUES (%s, %s, %s, %s, %s)
        """, (first, last, mid, uname, age))
        conn.commit()
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("Register Error:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

@app.get("/api/get_user")
def get_user():
    uid = request.args.get("user_id")
    if not uid:
        return jsonify({"error": "Missing user_id"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("""
            SELECT first_name, last_name, age
            FROM users
            WHERE user_id = %s
        """, (uid,))
        
        row = cur.fetchone()
        return jsonify(row if row else {})

    except Exception as e:
        print("get_user error:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

@app.get("/api/station_by_id")
def station_by_id():
    sid = request.args.get("id")
    if not sid:
        return jsonify({"error": "Missing id"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT station_id, location_name
            FROM stations
            WHERE station_id=%s
        """, (sid,))
        row = cur.fetchone()
        return jsonify(row if row else {})

    except Exception as e:
        print("station_by_id error:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

@app.get("/api/station_by_name")
def station_by_name():
    name = request.args.get("name")
    if not name:
        return jsonify({"error": "Missing name"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT station_id, location_name
            FROM stations
            WHERE location_name = %s
        """, (name,))
        row = cur.fetchone()
        return jsonify(row if row else {})

    except Exception as e:
        print("station_by_name error:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

@app.route("/api/adv_search", methods=["POST", "OPTIONS"])
def adv_search():
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200

    try:
        data = request.get_json(force=True)
        query = data.get("query", "")

        print("\n====================================")
        print("📌 RUNNING ADVANCED SEARCH QUERY:")
        print(query)
        print("====================================\n")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        # CLEAN JSON (convert timedelta, datetime → strings)
        cleaned_rows = []
        for row in rows:
            fixed = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date, time)):
                    fixed[k] = str(v)
                elif isinstance(v, timedelta):
                    total = int(v.total_seconds())
                    h = total // 3600
                    m = (total % 3600) // 60
                    s = total % 60
                    fixed[k] = f"{h:02d}:{m:02d}:{s:02d}"
                else:
                    fixed[k] = v
            cleaned_rows.append(fixed)

        return jsonify(cleaned_rows)

    except Exception as e:
        print("❌ ADV SEARCH ERROR:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    print(f"Starting Flask server at http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=True, use_reloader=False)
