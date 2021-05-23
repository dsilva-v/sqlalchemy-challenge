import numpy as np
import sqlalchemy
import pandas as pd

from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from flask import Flask, jsonify

# Setting up the database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflecting the database automatically into a new model
Base = automap_base()

# Reflecting the tables into the new model
Base.prepare(engine, reflect=True)

# Saving the references of the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Setting up Flask to use
app = Flask(__name__)

# Setting up Flask routes


@app.route("/")
def home():
    # Listing all the available routes
    return(
        f"Available routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Session or link from python to the DB
    session = Session(engine)
    prcp_query = session.query(Measurement.date, Measurement.prcp).filter(
        Measurement.date >= '2016-08-23')
    session.close()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_results = pd.read_sql(prcp_query.statement, engine)

    prcp_results.set_index('date', inplace=True)
    # Sort the dataframe by date
    prcp_results.sort_values(by='date', inplace=True)
    # sum all the values for a date using groupby on date
    sum_prcp_results = prcp_results.groupby('date').sum()

    # Convert to dictionary
    sum_prcp_results_dict = sum_prcp_results.to_dict()

    return jsonify(sum_prcp_results_dict)


@app.route("/api/v1.0/stations")
def stations():
    # Session or link from python to the DB
    session = Session(engine)
    station_query = session.query(Station.station)
    session.close()

    station_results = pd.read_sql(station_query.statement, engine)

    # Convert to dictionary
    station_results_dict = station_results.to_dict()

    return jsonify(station_results_dict)


@app.route("/api/v1.0/tobs")
def tobs():
    # Session or link from python to the DB
    session = Session(engine)
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(
        Measurement.station).order_by(func.count(Measurement.station).desc())

    most_active_station = (most_active_stations.all())[0][0]

    yearly_temp_data = session.query(Measurement.date, Measurement.tobs).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= '2016-08-23').all()

    yearly_temp_data = [
        item for temp_data in yearly_temp_data for item in temp_data]
    session.close()
    # Convert to dictionary
    # yearly_temp_data_dict = yearly_temp_data.to_dict()

    return jsonify(yearly_temp_data)


@app.route("/api/v1.0/<start>")
def temperatures_start(start):
    # Session or link from python to the DB
    session = Session(engine)
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(
        Measurement.station).order_by(func.count(Measurement.station).desc())

    most_active_station = (most_active_stations.all())[0][0]

    lowest_temp = session.query(func.min(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {lowest_temp} deg.')
    highest_temp = session.query(func.max(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {highest_temp} deg.')
    average_temp = session.query(func.avg(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {round(average_temp,1)} deg.')
    session.close()

    temp_start_list = [{"TMIN": lowest_temp,
                        "TAVG": highest_temp, "TMAX": average_temp}]

    return jsonify(temp_start_list)


@app.route("/api/v1.0/<start>/<end>")
def temp_start_end(start, end):
    # Session or link from python to the DB
    session = Session(engine)
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(
        Measurement.station).order_by(func.count(Measurement.station).desc())

    most_active_station = (most_active_stations.all())[0][0]

    lowest_temp = session.query(func.min(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {lowest_temp} deg.')
    highest_temp = session.query(func.max(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {highest_temp} deg.')
    average_temp = session.query(func.avg(Measurement.tobs)).filter(
        Measurement.station == most_active_station).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    print(
        f'The lowest temperature for the most active station {most_active_station} is {round(average_temp,1)} deg.')
    session.close()

    temp_start_list = [{"TMIN": lowest_temp,
                        "TAVG": highest_temp, "TMAX": average_temp}]

    return jsonify(temp_start_list)


if __name__ == '__main__':
    app.run(debug=True)
