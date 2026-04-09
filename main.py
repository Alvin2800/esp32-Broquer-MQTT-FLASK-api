from flask import Flask, jsonify
from datetime import datetime
import mysql.connector
import os

app = Flask(__name__)

db_config = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE")
}

distance = 0.0
alert = 0

def db_connection():
    return mysql.connector.connect(**db_config)

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
        print("table MQTT prête", flush=True)

    except Exception as e:
        print("erreur base donnée MQTT", e, flush=True)

init_db()

def insert_mqtt_data(timestamp, distance, alert):
    try:
        conn = db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO log_distance_mqtt (timestamp, distance, alert)
            VALUES (%s, %s, %s)
        """, (timestamp, distance, alert))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print("erreur insertion MQTT", e, flush=True)

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
        print("erreur logs MQTT", e, flush=True)
        return jsonify({"erreur": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
