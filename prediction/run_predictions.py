import h2o
import pandas as pd
import numpy as np
import json
import requests
import os
from datetime import datetime
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")


# initialise h2o
h2o.init()

# import postcode data
print("==============================================")
print("Importing Postcode Data")
print("==============================================")
postcode_data = pd.read_csv("australian_postcodes.csv")

# filter for vic postcodes
vic_postcodes = postcode_data[postcode_data["State"] == "VIC"]
vic_postcodes = vic_postcodes.drop_duplicates(subset="postcode")
vic_postcodes.head()

# create postcode data
postcode_list = []

for index, row in vic_postcodes.iterrows():
    if np.isnan(row["lat"]):
        continue
    else:
        postcode_dict = dict()
        postcode_dict["postcode"] = row["postcode"]
        postcode_dict["coordinates"] = [row["lat"], row["long"]]
        postcode_list.append(postcode_dict)

# get weather data for every postcode

# set api key for dark sky weather data
api_key = os.environ["WEATHER_API_KEY"]

# store weather data
weather_data = []

# loop through postcodes and get weather data
print("==============================================")
print("Importing Weather Data")
print("==============================================")
for postcode in postcode_list:
    lat = str(postcode["coordinates"][0])
    lon = str(postcode["coordinates"][1])
    suffix = lat + "," + lon + "?exclude=currently,minutely,hourly,alerts,flags"
    url = "https://api.darksky.net/forecast/" + api_key + "/" + suffix
    response = requests.get(url)
    weather_json = json.loads(response.text)
    weather_dict = dict()
    weather_dict["postcode"] = postcode["postcode"]
    weather_dict["data"] = weather_json
    weather_dict["latitude"] = postcode["coordinates"][0]
    weather_dict["longitude"] = postcode["coordinates"][1]
    weather_data.append(weather_dict)

# get postcode weather data in df and then convert to h2o frame
overall_df = pd.DataFrame()

for point in weather_data:
    try:
        df = pd.DataFrame()
        df = df.append(point["data"]["daily"]["data"][0], ignore_index=True)
        df["postcode"] = point["postcode"]
        df["latitude"] = point["latitude"]
        df["longitude"] = point["longitude"]
        overall_df = overall_df.append(df, ignore_index=True)
    except:
        continue

new_data = h2o.H2OFrame(overall_df)

# load model
model = h2o.load_model("gbm_grid2_model_28")

# generate predictions
print("==============================================")
print("Generating Predictions")
print("==============================================")
predictions = model.predict(new_data)

# append predictions to df
overall_df["prediction"] = predictions.as_data_frame()

# get necessary columns and edit dataframe
prediction_df = overall_df[["prediction", "temperatureHigh", "pressure", "humidity", "windSpeed", "postcode"]].copy()
prediction_df.rename(columns={"prediction": "bushfireProb",
                              "temperatureHigh": "airTemperature",
                              "pressure": "airPressure",
                              "humidity": "humidity",
                              "windSpeed": "windSpeed"}, inplace=True)

# convert fahrenheit to celsius
prediction_df["airTemperature"] = prediction_df["airTemperature"].apply(lambda x: (x - 32) * 5.0 / 9.0)

# convert miles per hour to km per hour
prediction_df["windSpeed"] = prediction_df["windSpeed"].apply(lambda x: x / 0.62137)

# convert humidity to percentage
prediction_df["humidity"] = prediction_df["humidity"] * 100


# create bushfire risk and severity rating
def bushfire_risk_rating(prob):
    if prob <= 0.33:
        risk_rating = "LOW"
    elif (prob > 0.33) & (prob <= 0.66):
        risk_rating = "MEDIUM"
    else:
        risk_rating = "HIGH"
    return risk_rating


def bushfire_severity_rating(prob):
    if prob <= 0.33:
        severity_rating = "MINIMAL"
    elif (prob > 0.33) & (prob <= 0.66):
        severity_rating = "MEDIUM"
    else:
        severity_rating = "STRONG"
    return severity_rating


prediction_df["bushfireRiskRating"] = prediction_df["bushfireProb"].map(bushfire_risk_rating)
prediction_df["bushfireSeverityRating"] = prediction_df["bushfireProb"].map(bushfire_severity_rating)

# add timestamp
prediction_df["lastUpdated"] = datetime.now()

# write to db
print("==============================================")
print("Writing to Database")
print("==============================================")
engine = create_engine(os.environ["CC_DB_PATH"])
prediction_df.to_sql(con=engine, name="predictions", if_exists="replace", index=False)

# close h2o cluster
h2o.cluster().shutdown()
