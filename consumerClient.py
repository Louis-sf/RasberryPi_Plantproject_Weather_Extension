import time
import matplotlib.pyplot as plt
from sklearn import linear_model
from IPython.display import display
import pandas as pd
from confluent_kafka import Consumer
import json

from sklearn.metrics import mean_squared_error

import ccloud_lib
from pandas import json_normalize
config_file = "python.config"
topic = "pksqlc-pj1gySIMPLETABLE"
conf = ccloud_lib.read_ccloud_config(config_file)

consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
consumer_conf['group.id'] = 'indefiniteconsumer'
consumer_conf['auto.offset.reset'] = 'earliest'
consumer_conf['enable.auto.commit'] = 'false'
weatherConsumer = Consumer(consumer_conf)
enriched_consumer = Consumer(consumer_conf)
enriched_consumer.unsubscribe()
enriched_consumer.subscribe(["pksqlc-pj1gyWEATHER_PLANT_ENRICHED"])
#{
#  "WEATHER_TIME": "2022-06-15 00:15:48",
#  "WEATHER_TIMESTAMP": 1655252148,
#  "WEATHER_HUMIDITY": 64,
#  "WEATHER_PRESSURE": 1021,
#  "SF_TEMPERATURE": 286.62,
#  "PLANT_ID": "5",
#  "PLANT_MOISTURE": 66,
#  "PLANT_TERMPERATURE": 27.378999710083008,
#  "PLANT_SCIENTIFIC_NAME": "Rhaphidophora tetrasperma",
#  "PLANT_COMMON_NAME": "Mini Monstera",
#  "PLANT_GIVEN_NAME": "Ginny",
#  "PLANT_TEMPERATURE_LOW": 15,
#  "PLANT_TEMPERATURE_HIGH": 30,
#  "PLANT_MOISTURE_LOW": 40,
#  "PLANT_MOISTURE_HIGH": 70
#}

#creating panda data frame for each of the plant to store information
df0 = pd.DataFrame({'WEATHER_TIME': pd.Series(dtype= 'str'),
                    'WEATHER_TIMESTAMP': pd.Series(dtype='int'),
                    'WEATHER_HUMIDITY': pd.Series(dtype='int'),
                    "WEATHER_PRESSURE": pd.Series(dtype='int'),
                    "SF_TEMPERATURE": pd.Series(dtype='float'),
                    "PLANT_ID": pd.Series(dtype='str'),
                    "PLANT_MOISTURE": pd.Series(dtype='float'),
                    "PLANT_TERMPERATURE": pd.Series(dtype='float'),
                    "PLANT_SCIENTIFIC_NAME": pd.Series(dtype='str'),
                    "PLANT_COMMON_NAME": pd.Series(dtype='str'),
                    "PLANT_GIVEN_NAME": pd.Series(dtype='str'),
                    "PLANT_TEMPERATURE_LOW": pd.Series(dtype='float'),
                    "PLANT_TEMPERATURE_HIGH": pd.Series(dtype='float'),
                    "PLANT_MOISTURE_LOW": pd.Series(dtype='float'),
                    "PLANT_MOISTURE_HIGH": pd.Series(dtype='float'),
                    })

count = 0
x_test = pd.DataFrame({'WEATHER_HUMIDITY': pd.Series(dtype='int')})
y_test = pd.DataFrame({"PLANT_MOISTURE": pd.Series(dtype='float')})
try:
    while True:
        msg = enriched_consumer.poll(1.0)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            #time.sleep(5)
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
            continue
        else:
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            #print("the enriched message has key{}, and value{}".format(record_key, record_value))
            data = json.loads(record_value)
            curr_value_list = list(data.values())
            df0.loc[len(df0)] = curr_value_list
            count = count + 1
            #print("data written")
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    df0 = df0.sort_values('WEATHER_TIMESTAMP', ascending=True)
    df0.to_csv(str(time.time()) + 'enriched_table')
    display(df0)
    enriched_consumer.close()
