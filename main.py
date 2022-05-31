from bluepy.btle import UUID, Peripheral, DefaultDelegate, AssignedNumbers
import struct
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import time

def _TI_UUID(val):
    return UUID("%08X-0451-4000-b000-000000000000" % (0xF0000000+val))

# Sensortag versions
AUTODETECT = "-"
SENSORTAG_V1 = "v1"
SENSORTAG_2650 = "CC2650"

class SensorBase:
    # Derived classes should set: svcUUID, ctrlUUID, dataUUID
    sensorOn  = struct.pack("B", 0x01)
    sensorOff = struct.pack("B", 0x00)

    def __init__(self, periph):
        self.periph = periph
        self.service = None
        self.ctrl = None
        self.data = None

    def enable(self):
        if self.service is None:
            self.service = self.periph.getServiceByUUID(self.svcUUID)
        if self.ctrl is None:
            self.ctrl = self.service.getCharacteristics(self.ctrlUUID) [0]
        if self.data is None:
            self.data = self.service.getCharacteristics(self.dataUUID) [0]
        if self.sensorOn is not None:
            self.ctrl.write(self.sensorOn,withResponse=True)

    def read(self):
        return self.data.read()

    def disable(self):
        if self.ctrl is not None:
            self.ctrl.write(self.sensorOff)

    # Derived class should implement _formatData()

def calcPoly(coeffs, x):
    return coeffs[0] + (coeffs[1]*x) + (coeffs[2]*x*x)

class HumiditySensorHDC1000(SensorBase):
    svcUUID  = _TI_UUID(0xAA20)
    dataUUID = _TI_UUID(0xAA21)
    ctrlUUID = _TI_UUID(0xAA22)

    def __init__(self, periph):
        SensorBase.__init__(self, periph)

    def read(self):
        '''Returns (ambient_temp, rel_humidity)'''
        (rawT, rawH) = struct.unpack('<HH', self.data.read())
        temp = -40.0 + 165.0 * (rawT / 65536.0)
        RH = 100.0 * (rawH/65536.0)
        return (temp, RH)

class OpticalSensorOPT3001(SensorBase):
    svcUUID  = _TI_UUID(0xAA70)
    dataUUID = _TI_UUID(0xAA71)
    ctrlUUID = _TI_UUID(0xAA72)

    def __init__(self, periph):
       SensorBase.__init__(self, periph)

    def read(self):
        '''Returns value in lux'''
        raw = struct.unpack('<h', self.data.read()) [0]
        m = raw & 0xFFF
        e = (raw & 0xF000) >> 12
        return 0.01 * (m << e)

class BatterySensor(SensorBase):
    svcUUID  = UUID("0000180f-0000-1000-8000-00805f9b34fb")
    dataUUID = UUID("00002a19-0000-1000-8000-00805f9b34fb")
    ctrlUUID = None
    sensorOn = None

    def __init__(self, periph):
       SensorBase.__init__(self, periph)

    def read(self):
        '''Returns the battery level in percent'''
        val = ord(self.data.read())
        return val

class SensorTag(Peripheral):
    def __init__(self,addr,version=AUTODETECT):
        Peripheral.__init__(self,addr)
        if version==AUTODETECT:
            svcs = self.discoverServices()
            if _TI_UUID(0xAA70) in svcs:
                version = SENSORTAG_2650
            else:
                version = SENSORTAG_V1

        fwVers = self.getCharacteristics(uuid=AssignedNumbers.firmwareRevisionString)
        if len(fwVers) >= 1:
            self.firmwareVersion = fwVers[0].read().decode("utf-8")
        else:
            self.firmwareVersion = u''

        self.humidity = HumiditySensorHDC1000(self)
        self.lightmeter = OpticalSensorOPT3001(self)
        self.battery = BatterySensor(self)

def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("Successfully connected to MQTT-Broker. Code=",rc)
        #client.subscribe(topic)
    else:
        print("Error connecting to the MQTT-Broker. Code=",rc)

def on_publish(client, userdata, result):
    print("Successfully published a message on MQTT")
    pass

def buildJson(listValues):
    timestamp = datetime.now().isoformat()

    jObservations = []
    for value in listValues:
        jOberservationEntry = {'SensorTypeCode': value[0], 'Measure': {'Content': value[1], 'UnitCode': value[2] }, 'DateTime': timestamp}
        jObservations.append(jOberservationEntry)

    jObject = {'ConditionMonitoring': {'AssetID': "Lego-BOT", 'Observation': jObservations}}
    return json.dumps(jObject)

def main():
    #Connecting to MQTT
    mqtt.Client.connected_flag = False
    mqtt_client_id = "MW220-MQTT-Client-Group-4"
    mqtt_host = "IWILR3-7.campus.fh-ludwigshafen.de"
    mqtt_port = 1883
    mqtt_topic = "Factory/ColorSorter/ConditionMonitoring"
    client = mqtt.Client(mqtt_client_id)
    client.on_connect = on_connect
    client.on_publish = on_publish
    
 
    print("Attempting to connect to MQTT-Broker...")
    client.loop_start()
    client.connect(mqtt_host, mqtt_port, 60)
    while not client.connected_flag:
        print("... connected: ", client.is_connected())
        time.sleep(1)


    # Connecting to TI SensorTag
    sensortag_mac = "98:07:2d:27:f1:86"
    print('Connecting to ' + sensortag_mac)
    tag = SensorTag(sensortag_mac)
    print("Successfully connected to SensorTag")

    tag.humidity.enable()
    tag.battery.enable()
    tag.lightmeter.enable()

    print("Successfully enabled all sensors. Starting Rotation...")
    time.sleep(1)

    while True:
        humidity = tag.humidity.read()
        light = tag.lightmeter.read()
        battery = tag.battery.read()
        print("Humidity: ", humidity)
        print("Light: ", light)
        print("Battery: ", battery)
        
        values = []
        values.append(["Temperature", humidity[0], "CEL"])
        values.append(["Humidity", humidity[1], "PCT"])
        values.append(["Light", light, "LMN"])
        values.append(["Battery", battery, "PCT"])
        json = buildJson(values)

        print(json)
        client.publish(mqtt_topic, json)

        time.sleep(5)

if __name__ == "__main__":
    main()