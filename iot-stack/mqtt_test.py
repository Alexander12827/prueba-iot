import paho.mqtt.client as mqtt
import time
import random
import json

broker = "localhost"  # o "mqtt" si es dentro de Docker
port = 1883
topic = "iot/sensor"

client = mqtt.Client(client_id="", protocol=mqtt.MQTTv311)
client.connect(broker, port, 60)

while True:
    payload = json.dumps({
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(40, 60), 2)
    })
    client.publish(topic, payload)
    print(f"Published: {payload}")
    time.sleep(5)

