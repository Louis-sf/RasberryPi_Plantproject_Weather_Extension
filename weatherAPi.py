import requests
import os
from datetime import datetime
import time
from confluent_kafka import Producer
import ccloud_lib
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
from kafka import KafkaProducer

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    sfproducer = Producer(producer_conf)
    delivered_records = 0
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    #basic info for openmapweather.org
    user_api = "fd4c2e4f0ab38bd7bde89d74d0afe7de"
    lattitude = "47"
    longitude = "-122"

    location = "San Francisco"
    while True:
        complete_api_link = "https://api.openweathermap.org/data/2.5/weather?lat="+lattitude +"&lon=" + longitude +\
                           "&appid=" + user_api
        #complete_api_link = "api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=fd4c2e4f0ab38bd7bde89d74d0afe7de"
        api_link = requests.get(complete_api_link)
        api_data = api_link.json()
        #print(api_data)
        delivered_records = delivered_records + 1
        humidity = api_data["main"]["humidity"]
        pressure = api_data["main"]["pressure"]
        temperature = api_data["main"]["temp"]
        time_stamp = time.time()
        #info = {humidity, pressure, temperature, time}
        Schema = {'humidity': humidity,
                  'pressure': pressure,
                  'temperature': temperature,
                  'time stamp': time_stamp}
        record_value = json.dumps(Schema)
        sfproducer.produce(topic, key=str(delivered_records), value=record_value, on_delivery=acked)
        #sfproducer.poll()
        #print(record_value)
        print("{} messages were produced to topic {}!".format(delivered_records, topic))
        time.sleep(15*60)
        #time.sleep(60*15)
        #print(api_data)
        #break