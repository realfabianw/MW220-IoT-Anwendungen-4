import struct
import math
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import time
import sys

def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("connected OK Returned code=",rc)
        #client.subscribe(topic)
    else:
        print("Bad connection Returned code= ",rc)

def main():
    
    mqtt.Client.connected_flag = False
    mqtt_client_id = "MW220-MQTT-Client-Group-4"
    mqtt_host = "localhost"
    mqtt_port = 1883
    mqtt_topic = "testtopic/mw220"

    client = mqtt.Client(mqtt_client_id)
    
    client.on_connect = on_connect

    print("Attempting to connect to MQTT-Broker...")
    client.loop_start()
    client.connect(mqtt_host, mqtt_port, 60)
    while not client.connected_flag:
        print("... connected: ", client.is_connected())
        time.sleep(1)
    
    client.publish(mqtt_topic, json.dumps({'key': 1}))
    # client.loop_stop()




if __name__ == "__main__":
    main()

