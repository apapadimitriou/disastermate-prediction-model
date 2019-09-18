import traceback
import pandas as pd
import numpy as np
import json
import requests
import os
from datetime import datetime
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

try:
    # import postcode data
    print("==============================================")
    print("Importing Postcode Data")
    print("==============================================")
    postcode_data = pd.read_csv("australian_postcodes.csv")

    # filter for vic postcodes
    vic_postcodes = postcode_data[postcode_data["State"].isin(["VIC", "NSW", "QLD"])]
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

    # get necessary columns and edit dataframe
    prediction_df = overall_df[["temperatureHigh", "pressure",
                                "humidity", "windSpeed", "precipIntensityMax", "postcode"]].copy()
    prediction_df.rename(columns={"prediction": "floodProb",
                                  "temperatureHigh": "airTemperature",
                                  "pressure": "airPressure",
                                  "humidity": "humidity",
                                  "precipIntensityMax": "rainfall"}, inplace=True)

    # convert fahrenheit to celsius
    prediction_df["airTemperature"] = prediction_df["airTemperature"].apply(lambda x: (x - 32) * 5.0 / 9.0)
    prediction_df["airTemperature"] = prediction_df["airTemperature"].astype("int64")

    # convert miles per hour to km per hour
    prediction_df["windSpeed"] = prediction_df["windSpeed"].apply(lambda x: x / 0.62137)
    prediction_df["windSpeed"] = prediction_df["windSpeed"].astype("int64")

    # convert humidity to percentage
    prediction_df["humidity"] = prediction_df["humidity"] * 100
    prediction_df["humidity"] = prediction_df["humidity"].astype("int64")

    # convert rainfall to mm
    prediction_df["rainfall"] = prediction_df["rainfall"] * 25.4
    prediction_df["rainfall"] = prediction_df["rainfall"].astype("int64")

    # create flood risk and severity rating
    def flood_risk_rating(rainfall):
        if rainfall <= 20:
            risk_rating = "LOW"
        elif (rainfall > 20) & (rainfall <= 50):
            risk_rating = "MEDIUM"
        else:
            risk_rating = "HIGH"
        return risk_rating


    def flood_severity_rating(rainfall):
        if rainfall <= 20:
            severity_rating = "MINIMAL"
        elif (rainfall > 20) & (rainfall <= 50):
            severity_rating = "MEDIUM"
        else:
            severity_rating = "STRONG"
        return severity_rating


    prediction_df["floodRiskRating"] = prediction_df["rainfall"].map(flood_risk_rating)
    prediction_df["floodSeverityRating"] = prediction_df["rainfall"].map(flood_severity_rating)

    # add timestamp
    prediction_df["lastUpdated"] = datetime.now()

    # write to db
    print("==============================================")
    print("Writing to Database")
    print("==============================================")
    engine = create_engine(os.environ["CC_DB_PATH"])
    prediction_df.to_sql(con=engine, name="flood_predictions", if_exists="replace", index=False)

    # send message to slack channel
    error_message = "Flood Prediction Model Run Successfully"
    slackurl = os.environ["SLACK_HOOK"]
    slackpayload = {'text': error_message}
    response = requests.post(slackurl, data=json.dumps(slackpayload), headers={'Content-Type': 'application/json'})
except:
    # send error to slack channel
    # error_message = "Flood Prediction Model Failed"
    error_message = traceback.format_exc()
    slackurl = os.environ["SLACK_HOOK"]
    slackpayload = {'text': error_message}
    response = requests.post(slackurl, data=json.dumps(slackpayload), headers={'Content-Type': 'application/json'})
