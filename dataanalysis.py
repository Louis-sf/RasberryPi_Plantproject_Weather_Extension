import time
import matplotlib.pyplot as plt
from sklearn import linear_model
from IPython.display import display
import pandas as pd
from confluent_kafka import Consumer
import json

from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

pd.set_option('display.max_columns', None)

plant_weather_enriched = pd.read_csv('1656090650.010932enriched_table', index_col= 0)
display(plant_weather_enriched)
#filter dataframe by plant_id
plant0 = plant_weather_enriched[plant_weather_enriched['PLANT_ID'] == 0]
plant1 = plant_weather_enriched[plant_weather_enriched['PLANT_ID'] == 1]
plant2 = plant_weather_enriched[plant_weather_enriched['PLANT_ID'] == 2]
plant3 = plant_weather_enriched[plant_weather_enriched['PLANT_ID'] == 3]
plant4 = plant_weather_enriched[plant_weather_enriched['PLANT_ID'] == 4]
#sampling with fraction 8:2
X_train, X_test, y_train, y_test = train_test_split(plant0[['WEATHER_HUMIDITY','SF_TEMPERATURE']], plant0['PLANT_MOISTURE'],
                                                    test_size=0.2, random_state=1)
#fit regression
plant0_reg = linear_model.LinearRegression()
plant0_reg.fit(X_train, y_train)
plant_moisture_pred = plant0_reg.predict(X_test)
print("Mean squared error: %.2f" % mean_squared_error(y_test, plant_moisture_pred))
#create scatter plot + regression line
scatter = plt.plot(X_test, y_test, 'o')
# x = plant0[['WEATHER_HUMIDITY']]
# y = plant0['PLANT_MOISTURE']
# regr = linear_model.LinearRegression()
# regr.fit(x, y)
# time.sleep(0.5)
# x_test.loc[len(x_test)] = (data['WEATHER_HUMIDITY'])
# y_test.loc[len(y_test)] = (data['PLANT_MOISTURE'])
# y_pred = regr.predict(x_test)
# print("Mean squared error: %.2f" % mean_squared_error(y_test, y_pred))
# plt.scatter(x, y, color="black")
# plt.show()