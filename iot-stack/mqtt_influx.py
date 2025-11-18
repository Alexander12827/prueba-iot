import random
import time
import json
from paho.mqtt import client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision

# -------------------
# Configuración MQTT
# -------------------
BROKER = "127.0.0.1"  # si Python corre en host Linux
PORT = 1883
TOPIC = "iot/sensor"

# -------------------
# Configuración InfluxDB
# -------------------
INFLUX_URL = "http://localhost:8086"  # si Python corre en host Linux
INFLUX_TOKEN = "hbi_vn2i9YI5_pb3C09c-ztSxbZSFvJWTwh9Z-IZbEpm7WRQufDhULn5PaHbx5JGh81Lra85VzWgUvg_cSqewQ=="
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "iotdata"

# -------------------
# Conexiones
# -------------------
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api()

client = mqtt.Client(client_id="mqtt_influx_bridge")

# -------------------
# Callbacks
# -------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Conectado a MQTT broker!")
        client.subscribe(TOPIC)
        print(f"Suscrito al topic: {TOPIC}")
    else:
        print(f"❌ Error al conectar con MQTT, código: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        point = Point("sensor_data") \
            .tag("topic", msg.topic) \
            .field("temperature", payload.get("temperature")) \
            .field("humidity", payload.get("humidity")) \
            .time(time.time_ns(), WritePrecision.NS)
        write_api.write(INFLUX_BUCKET, INFLUX_ORG, point)
        print(f"📤 Escrito en InfluxDB: {payload}")
    except Exception as e:
        print(f"⚠️ Error procesando mensaje: {e}")

# -------------------
# Inicialización
# -------------------
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, keepalive=60)
client.loop_forever()
