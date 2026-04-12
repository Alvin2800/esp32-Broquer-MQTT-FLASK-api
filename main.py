from flask import Flask, jsonify
from datetime import datetime
import mysql.connector
import os
import json
import paho.mqtt.client as mqtt

app = Flask(__name__)

db_config = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE")
}

# Variables globales pour /status
distance = 0.0
alert = 0

# Config MQTT
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "alvin/iot/fuel_level"


# Connexion à MySQL
def db_connection():
    return mysql.connector.connect(**db_config)


# Création de la table dédiée MQTT
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

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ table MQTT prête", flush=True)

    except Exception as e:
        print("❌ erreur base donnée MQTT :", e, flush=True)


# Insertion des données MQTT
def insert_mqtt_data(timestamp, distance_value, alert_value):
    try:
        conn = db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO log_distance_mqtt (timestamp, distance, alert)
            VALUES (%s, %s, %s)
        """, (timestamp, distance_value, alert_value))

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ données MQTT insérées en base", flush=True)

    except Exception as e:
        print("❌ erreur insertion MQTT :", e, flush=True)


# Callback MQTT : quand un message arrive
def on_message(client, userdata, msg):
    global distance, alert

    try:
        payload = msg.payload.decode()
        print("📩 message MQTT brut reçu :", payload, flush=True)

        data = json.loads(payload)

        distance = float(data.get("distance", 0))
        alert = int(data.get("alert", 0))
        timestamp = datetime.now()

        insert_mqtt_data(timestamp, distance, alert)

        print("📡 MQTT reçu et traité :", {
            "distance": distance,
            "alert": alert,
            "timestamp": str(timestamp)
        }, flush=True)

    except Exception as e:
        print("❌ erreur traitement MQTT :", e, flush=True)


# Démarrage MQTT subscriber
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


# Initialisation
init_db()
start_mqtt()


@app.route("/")
def home():
    return "MQTT API IS RUNNING"


@app.route("/status")
def status():
    return jsonify({
        "distance": distance,
        "alert": alert
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

        log = []
        for row in rows:
            log.append({
                "timestamp": str(row[0]),
                "distance": row[1],
                "alert": row[2]
            })

        return jsonify(log)

    except Exception as e:
        print("❌ erreur logs MQTT :", e, flush=True)
        return jsonify({"erreur": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
