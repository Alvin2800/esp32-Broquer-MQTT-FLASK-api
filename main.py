from flask import Flask, jsonify
import os
from datetime import datetime
import mysql.connector
import json
import paho.mqtt.client as mqtt

app = Flask(__name__)

# DB config
db_config = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "database": os.getenv("MYSQLDATABASE"),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD")
}

def db_connection():
    return mysql.connector.connect(**db_config)

# MQTT
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "alvin/iot/fuel_level"

distance = 0.0
alert = 0
last_distance = None

def init_db():
    conn = db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS log_distance_mqtt(
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        distance FLOAT NOT NULL,
        alert INT NOT NULL
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

init_db()

def insert_data(timestamp, distance, alert):
    conn = db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO log_distance_mqtt (timestamp, distance, alert)
    VALUES (%s, %s, %s)
    """, (timestamp, distance, alert))

    conn.commit()
    cursor.close()
    conn.close()

def detect_anomaly(current, previous, threshold=50):
    if previous is None:
        return False
    return abs(current - previous) > threshold

def on_message(client, userdata, msg):
    global distance, alert, last_distance

    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        distance = float(data.get("distance", 0))
        alert = int(data.get("alert", 0))
        timestamp = datetime.now()

        anomaly = detect_anomaly(distance, last_distance)
        last_distance = distance

        insert_data(timestamp, distance, alert)

        print("DATA:", distance, "ANOMALY:", anomaly)

    except Exception as e:
        print("Erreur:", e)

def start_mqtt():
    client = mqtt.Client()
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)

    client.loop_start()

start_mqtt()

@app.route("/")
def home():
    return "API running"

@app.route("/status")
def status():
    return jsonify({"distance": distance, "alert": alert})

@app.route("/logs")
def logs():
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
            "alert": row[2]
        })

    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
