import os
import googlemaps
import pandas as pd
from jax import grad
import jax.numpy as jnp
from datetime import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv()

#Function to handle NULL/BLANK in Integer data types.
def get_integer_value(value):
    if value == '':
        return None
    else:
        return int(value)

#Function to handle NULL/BLANK in Float data types.
def get_float_value(value):
    if value == '':
        return None
    else:
        return float(value)

#Function to handle NULL/BLANK in Date data types.
def get_date_value(value):
    if value == '':
        return None
    else:
        return dt.strptime(value, '%Y-%m-%d')

try:
    #Establish connection.
    RAILWAY_DB_URL = os.getenv("RAILWAY_DB_URL")
    engine = create_engine(RAILWAY_DB_URL, connect_args={'options': '-c search_path=transport_pro'})

    #Initialize Google Maps API
    GOOGLE_API = os.getenv("GOOGLE_API")
    gmap = googlemaps.Client(key=GOOGLE_API)

    #Route optimization algorithm.
    #Getting routes from Google Maps
    def get_routes(api_key, start_lat, start_long, end_lat, end_long):
        gmap = googlemaps.Client(key=api_key)
        origin = (start_lat, start_long)
        destination = (end_lat, end_long)
        routes = gmap.directions(
            origin=origin,
            destination=destination,
            mode='driving',
            alternatives=True
        )
        return routes

    #Extracting distance and time from routes
    def get_distance_and_time(routes):
        distance_time_list = []
        for route in routes:
            leg = route['legs'][0]
            distance_km = leg['distance']['value'] / 1000 #To get distance in KM.
            duration_min = leg['duration']['value'] / 60 #To get duration in MIN.
            distance_time_list.append((distance_km, duration_min))
        return distance_time_list

    #Cost function
    def cost_function_for_route(distance_km, duration_min, fuel_cost_per_km, time_weight):
        base = (distance_km * duration_min) + (fuel_cost_per_km * time_weight)
        return base ** 2

    #Gradient descent algorithm
    #Function to identify minimum of costs.
    def total_cost(time_weight, distance_time_list, fuel_cost_per_km):
        for d,t in distance_time_list:
            costs = [cost_function_for_route(d, t, fuel_cost_per_km, time_weight)]
        return jnp.min((jnp.array(costs)))

    #Function for gradient descent.
    def optimize_weight(initial_weight, distance_time_list, fuel_cost_per_km, learning_rate=0.01, iterations=100):
        weight = initial_weight
        cost_grad = grad(total_cost)
        for i in range(iterations):
            g = cost_grad(weight, distance_time_list, fuel_cost_per_km)
            weight -= learning_rate * g
        return weight

    #Query to create new records.
    #Query to create a new route.
    new_route_fleet = get_integer_value(input('Select Fleet: '))
    new_route_start_loc = input('Starting Location: ') #This will be generated when the user enters the starting location in the front-end Google Map rendering.
    new_route_end_loc = input('Destination Location: ') #This will be generated when the user enters the starting location in the front-end Google Map rendering.
    new_route_per_km_fuel_cost = get_float_value(input('Per Km Fuel Cost: '))

    #Generate lat/long coordinates based on location inputs.
    geocode_result_start = gmap.geocode(new_route_start_loc)
    geocode_result_end = gmap.geocode(new_route_end_loc)
    geolocation_start = geocode_result_start[0]['geometry']['location']
    geolocation_end = geocode_result_end[0]['geometry']['location']
    new_route_start_lat = geolocation_start['lat']
    new_route_start_long = geolocation_start['lng']
    new_route_end_lat = geolocation_end['lat']
    new_route_end_long = geolocation_end['lng']

    #Calling route optimization algo
    routes = get_routes(api_key= GOOGLE_API, start_lat= new_route_start_lat, start_long= new_route_start_long, end_lat= new_route_end_lat, end_long= new_route_end_long)
    distance_time_list = get_distance_and_time(routes=routes)
    #Optimize weights
    optimal_weight = optimize_weight(initial_weight=0.01, distance_time_list=distance_time_list, fuel_cost_per_km=new_route_per_km_fuel_cost)
    #Apply optimal weight to cost function
    for d, t in distance_time_list:
        costs = [cost_function_for_route(distance_km=d, duration_min=t, fuel_cost_per_km=new_route_per_km_fuel_cost, time_weight=optimal_weight)]
    #Optimal route
    best_index = int(jnp.argmin(jnp.array(costs)))
    b_r_i = best_index - 1 #To get the best route from the list of routes (-1 since index start is 0)
    best_routes = routes[b_r_i]
    best_route_polyline = best_routes['overview_polyline']['points']
    best_distance_mtr = best_routes['legs'][0]['distance']['value']
    best_distance_km = best_distance_mtr / 1000
    best_time_sec = best_routes['legs'][0]['duration']['value']
    best_time_min = best_time_sec / 60

    new_route_optimized_time_min = best_time_min #This will be returned from the gradient descent algorithm above.
    new_route_optimized_total_distance_km = best_distance_km #This will be returned from the gradient descent algorithm above.
    insert_new_route = text("""
                                    insert into route_master(
                                        secondary_key_fleet,
                                        starting_location,
                                        ending_location,
                                        optimized_total_distance_km,
                                        optimized_route_time_min,
                                        per_km_fuel_cost
                                    )values(
                                        :secondary_key_fleet,
                                        :starting_location,
                                        :ending_location,
                                        :optimized_total_distance_km,
                                        :optimized_route_time_min,
                                        :per_km_fuel_cost
                                    )""")
    with engine.begin() as conn:
        conn.execute(insert_new_route, {
                                        'secondary_key_fleet': new_route_fleet,
                                        'starting_location': new_route_start_loc,
                                        'ending_location': new_route_end_loc,
                                        'optimized_total_distance_km': float(new_route_optimized_total_distance_km),
                                        'optimized_route_time_min': float(new_route_optimized_time_min),
                                        'per_km_fuel_cost': new_route_per_km_fuel_cost
        })

    #Query the view to use in the display results area.
    query_display = 'select * from route_master;'
    df = pd.read_sql(query_display, engine)
    print(f'Route Data:\n{df}')
except Exception as e:
    print(f'Failed to connect or query:{e}')