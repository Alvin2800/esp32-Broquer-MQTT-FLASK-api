from flask import Flask, jsonify
import os
from datetime import datetime
import mysql.connector
import json
import paho.mqtt.client as mqtt

app = Flask(__name__)

# =========================
# CONFIG DB
# =========================
db_config = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "database": os.getenv("MYSQLDATABASE"),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD")
}

def db_connection():
    return mysql.connector.connect(**db_config)

# =========================
# CONFIG MQTT
# =========================
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "alvin/iot/fuel_level"

# =========================
# VARIABLES GLOBALES
# =========================
distance = 0.0
alert = 0
event_type = "NORMAL"

last_distance = None
event_active = False
reference_distance = None
event_counter = 0
event_direction = None  # "RISE" ou "DROP"

# =========================
# INIT DB
# =========================
def init_db():
    try:
        conn = db_connection()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS log_distance_mqtt (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            distance FLOAT NOT NULL,
            alert INT NOT NULL
        )
        """)

        # ajoute event_type si absent
        cursor.execute("SHOW COLUMNS FROM log_distance_mqtt LIKE 'event_type'")
        if not cursor.fetchone():
            cursor.execute("""
            ALTER TABLE log_distance_mqtt
            ADD COLUMN event_type VARCHAR(50) NOT NULL DEFAULT 'NORMAL'
            """)
            print("✅ colonne event_type ajoutée", flush=True)

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ table MQTT prête", flush=True)

    except Exception as e:
        print("❌ erreur init DB :", e, flush=True)

init_db()

# =========================
# INSERT DATA
# =========================
def insert_data(timestamp, distance_value, alert_value, event_type_value):
    try:
        conn = db_connection()
        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO log_distance_mqtt (timestamp, distance, alert, event_type)
        VALUES (%s, %s, %s, %s)
        """, (timestamp, distance_value, alert_value, event_type_value))

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ donnée insérée en DB", flush=True)

    except Exception as e:
        print("❌ erreur insertion DB :", e, flush=True)

# =========================
# IA METIER
# =========================
def classify_event(current_distance):
    global last_distance, event_active, reference_distance, event_counter, event_type, event_direction

    BRUTAL_THRESHOLD = 100
    RETURN_THRESHOLD = 20
    OBSERVATION_WINDOW = 15

    # première mesure
    if last_distance is None:
        last_distance = current_distance
        event_type = "NORMAL"
        return event_type

    diff = current_distance - last_distance

    # aucun événement en cours
    if not event_active:
        # hausse brutale
        if diff >= BRUTAL_THRESHOLD:
            event_active = True
            reference_distance = last_distance
            event_counter = 0
            event_direction = "RISE"
            event_type = "SUSPECT_EVENT"

        # baisse brutale
        elif diff <= -BRUTAL_THRESHOLD:
            event_active = True
            reference_distance = last_distance
            event_counter = 0
            event_direction = "DROP"
            event_type = "SUSPECT_EVENT"

        else:
            event_type = "NORMAL"

    # événement en cours
    else:
        event_counter += 1

        # retour proche de la valeur avant événement
        if abs(current_distance - reference_distance) <= RETURN_THRESHOLD:
            event_type = "FAKE_ANOMALY"
            event_active = False
            reference_distance = None
            event_counter = 0
            event_direction = None

        # fin de fenêtre d'observation
        elif event_counter >= OBSERVATION_WINDOW:
            if event_direction == "RISE":
                event_type = "PROBABLE_THEFT"
            elif event_direction == "DROP":
                event_type = "REFUEL"
            else:
                event_type = "NORMAL"

            event_active = False
            reference_distance = None
            event_counter = 0
            event_direction = None

        else:
            event_type = "SUSPECT_EVENT"

    last_distance = current_distance
    return event_type

# =========================
# CALLBACK MQTT
# =========================
def on_message(client, userdata, msg):
    global distance, alert, event_type

    try:
        payload = msg.payload.decode()
        print("📩 message MQTT brut reçu :", payload, flush=True)

        data = json.loads(payload)

        distance = float(data.get("distance", 0))
        alert = int(data.get("alert", 0))
        timestamp = datetime.now()

        event_type = classify_event(distance)

        insert_data(timestamp, distance, alert, event_type)

        print("📡 MQTT reçu et traité :", {
            "distance": distance,
            "alert": alert,
            "event_type": event_type,
            "timestamp": str(timestamp)
        }, flush=True)

    except Exception as e:
        print("❌ erreur traitement MQTT :", e, flush=True)

# =========================
# START MQTT
# =========================
def start_mqtt():
    try:
        client = mqtt.Client()
        client.on_message = on_message

        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.subscribe(MQTT_TOPIC)

        print(f"✅ connecté au broker MQTT : {MQTT_BROKER}", flush=True)
        print(f"✅ abonné au topic : {MQTT_TOPIC}", flush=True)

        client.loop_start()

    except Exception as e:
        print("❌ erreur connexion MQTT :", e, flush=True)

start_mqtt()

# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return "MQTT API running"

@app.route("/status")
def status():
    return jsonify({
        "distance": distance,
        "alert": alert,
        "event_type": event_type
    })

@app.route("/logs")
def logs():
    try:
        conn = db_connection()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT timestamp, distance, alert
        FROM log_distance_mqtt
        ORDER BY timestamp DESC
        LIMIT 100
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "timestamp": str(row[0]),
                "distance": row[1],
                "alert": row[2],
            })

        return jsonify(data)

    except Exception as e:
        print("❌ erreur logs :", e, flush=True)
        return jsonify({"error": str(e)}), 500

# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
