from flask import Flask, jsonify
import os
from datetime import datetime
import mysql.connector
import json
import paho.mqtt.client as mqtt

app=Flask(__name__)

# declaration variable bd sur railway
db_config={
    "host": os.getenv("MYSQLHOST"),
    "port":os.getenv("MYSQLPORT",3306),
    "database":os.getenv("MYSQLDATABASE"),
    "user":os.getenv("MYSQLUSER"),
    "password":os.getenv("MYSQLPASSWORD")
}

def db_connection():
    return mysql.connector.connect(**db_config)



# config MQTT
MQTT_BROKER="test.mosquitto.org"
MQTT_PORT=1883
MQTT_TOPIC="alvin/iot/fluel_level"

distance=0.0
alerte=0
last_distance = None

def init_db():
    try:
        conn=db_connection()
        cursor=conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS log_distance_mqtt(
                       id INT AUTO_INCREMENT PRIMARY KEY,
                       timestamp DATETIME NOT NULL
                       distance float Not null,
                       alerte int Not null
                       )

                      """)
        conn.commit()
        cursor.close()
        conn.close()
        print("table MQTT prete", flush=True)
    except Exception as e:
        print("erreur base de donnée", flush=True)

init_db()
def insert_data():
    try:
        conn=db_connection()
        cursor=conn.cursor()
        timestamp=datetime.now()
        cursor.execute("""
        INSERT INTO log_distance_mqtt (timestamp,distance,alerte)
                       values ("%s","%s","%s")
                    """, (timestamp,distance,alerte))
        conn.commit()
        cursor.close()
        conn.close()
        print("data inseré",flush=True)
    except Exception as e:
        print("erreur insertion table",e,flush=True)

def detect_anomaly(current, previous, threshold=20):
    if previous is None:
        return False
    return abs(current - previous) > threshold


def on_message(client, userdata, msg):
    global distance, alert, last_distance

    try:
        payload = msg.payload.decode()
        print("📩 MQTT brut :", payload, flush=True)

        data = json.loads(payload)

        distance = float(data.get("distance", 0))
        alert = int(data.get("alert", 0))
        timestamp = datetime.now()

        # 🔥 IA ici
        anomaly = detect_anomaly(distance, last_distance)

        # mise à jour historique
        last_distance = distance

        # stockage (tu peux ajouter anomaly en DB plus tard)
        insert_data(timestamp, distance, alert)

        print("📡 DATA :", {
            "distance": distance,
            "anomaly": anomaly
        }, flush=True)

        if anomaly:
            print("🚨 ANOMALIE DETECTEE !!!", flush=True)

    except Exception as e:
        print("❌ erreur :", e, flush=True)
def start_mqtt():# fonction permettant de demarer la connexion mqtt coté flask
    try:
        client=mqtt.Client()# creation de client mqtt pour se connecter au broker
        client.on_message=on_message   

        client.connect(MQTT_BROKER,MQTT_PORT,60)
        client.subscribe(MQTT_TOPIC)
        print(f"connecté au broker MQTT {MQTT_BROKER}",flus=True)


        client.loop_start()
    except Exception as e:
        print("erreur connection au subscriber", e, flush=True)

@app.route("/")
def home():
    return "api is running"

@app.route("/status")
def status():
    return jsonify({"distance":distance,
                   "alerte": alerte})
@app.route("/logs")
def logs():
    try:
        conn=db_connection()
        cursor=conn.cursor()
        cursor.execute("""
        SELECT timestamp,distance,alerte FROM log_distance_mqtt order by timestamp DESC limit 100
                    """)
        rows=cursor.fetchall()
        cursor.close()
        conn.close()
        log=[]
        for row in rows:
            log.append({
                "timestamp":row[0],
                "distance":row[1],
                "alerte": row[2]}) 
        return jsonify(log)
    except Exception as e:
        print("erreur de log",e,flush=True)
        return jsonify({f"erreur tableau", str({e})})
    if _name_=="_main_":
        app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)))
