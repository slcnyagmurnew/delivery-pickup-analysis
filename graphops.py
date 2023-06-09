import math
import os

from redis import Redis
import polyline
import h3
import requests
from redisgraph import Graph


OSRM_HOST = os.getenv("OSRM_HOST", default=None)
OSRM_PORT = os.getenv("OSRM_PORT", default=None)
NUM_OF_ALTERNATIVES = os.getenv("ROUTE_ALTERNATIVES")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))


r = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT
)


def load_catboost_regressor_model():
    from catboost import CatBoostRegressor
    m = CatBoostRegressor()
    m.load_model("model/cb.pkl")
    return m


def check_location_pairs_exist_in_graph(source_hex_id, destination_hex_id):
    """
    Check if there is an edge between restaurant and delivery locations
    :param source_hex_id: restaurant hex id
    :param destination_hex_id: delivery hex id
    :return: existence (true, false), result set of Redis query (None, [[duration]])
    """
    graph = Graph(
        name="DistGraph",
        redis_con=r
    )
    params = {
        "source_hex_id": source_hex_id,
        "destination_hex_id": destination_hex_id
    }
    q = """MATCH (s:source)-[d:delivery]->(g:destination) WHERE s.hexId = $source_hex_id AND g.hexId = $destination_hex_id RETURN d.duration"""
    result = graph.query(q=q, params=params)
    result.pretty_print()
    return result.is_empty(), result.result_set


def get_route(res_hex_id, del_hex_id):
    """
    Get route from start location to end location
    Generate alternative routes (if exist)
    :param res_hex_id:
    :param del_hex_id:
    :return: list of dictionary of routes
    """
    start_lat, start_lon = h3.h3_to_geo(res_hex_id)  # get hex ids of lats and longs
    end_lat, end_lon = h3.h3_to_geo(del_hex_id)

    loc = "{},{};{},{}".format(start_lon, start_lat, end_lon, end_lat)
    url = f"http://{OSRM_HOST}:{OSRM_PORT}/route/v1/driving/"
    opts = f"?alternatives={NUM_OF_ALTERNATIVES}"

    request = requests.get(url + loc + opts)  # send request to OSRM backend
    if request.status_code != 200:
        return {"status": request.status_code}

    res = request.json()

    # print(res)

    # TODO compare distances if durations are almost same
    routes = []

    for index, gr in enumerate(res['routes']):
        generated_route = {"route": polyline.decode(gr['geometry']),
                           "distance": gr["distance"],
                           "duration": gr["duration"],
                           "start_point": [res['waypoints'][0]['location'][1], res['waypoints'][0]['location'][0]],
                           "end_point": [res['waypoints'][1]['location'][1], res['waypoints'][1]['location'][0]]
                           }
        routes.append(generated_route)

    routes = sorted(routes, key=lambda k: (k["duration"], k["distance"]), reverse=False)

    """For single result of fastest way of OSRM"""
    # start_point = [res['waypoints'][0]['location'][1], res['waypoints'][0]['location'][0]]
    # end_point = [res['waypoints'][1]['location'][1], res['waypoints'][1]['location'][0]]
    # distance = res['routes'][0]['distance']
    # duration = res['routes'][0]['duration']

    # out = {'route': routes,
    #        'start_point': start_point,
    #        'end_point': end_point
    #        }

    # print(routes)
    return routes


def predict_delivery(data: dict, hex_data: dict):
    """
    Predict delivery time for different conditions
    If location pair:
     - exists ->  in Redis Graph, use it with OSRM result, return the mean of them
     - not exists -> load CatBoost Regressor model and predict duration
    :param data: conditions of delivery operation (for non-existing pair)
    :param hex_data: hex ids of source and destination locations (for existing pair)
    :return:
    """
    empty, result_set = check_location_pairs_exist_in_graph(source_hex_id=hex_data["Restaurant_hex_id"],
                                                            destination_hex_id=hex_data["Delivery_hex_id"])

    # get osrm result for both two options
    osrm_results = get_route(res_hex_id=hex_data["Restaurant_hex_id"],
                             del_hex_id=hex_data["Delivery_hex_id"])
    optimal_result = math.ceil(osrm_results[0]["duration"] / 60)
    print(f"Road result: {optimal_result}")
    if not empty:
        graph_duration = result_set[0][0]
        print(f"Existing result in Graph: {graph_duration}")
        print(f"Difference between graph data and OSRM result: {abs(graph_duration - optimal_result)}")
        return (graph_duration + optimal_result) / 2, True
    else:
        print("Not exists in Redis Graph, prediction started..")
        m = load_catboost_regressor_model()  # predict duration with model
        model_duration = m.predict(list(data.values()))
        print(f"Model prediction result: {model_duration}")
        print(f"Difference between prediction data and OSRM result: {abs(model_duration - optimal_result)}")
        return (model_duration + optimal_result) / 2, False


def update_edge_of_graph(hex_data, new_duration):
    """
    Update existing edge duration value to new (obtained) duration
    :param hex_data: dict-> contains restaurant hex id and delivery hex id
    :param new_duration: calculated duration in minutes
    :return:
    """
    graph = Graph(
        name="DistGraph",
        redis_con=r
    )
    params = {"source_hex_id": hex_data["Restaurant_hex_id"],
              "destination_hex_id": hex_data["Delivery_hex_id"],
              "duration": new_duration}
    q = """MATCH (s:source)-[d:delivery]->(g:destination) WHERE s.hexId = $source_hex_id AND g.hexId = $destination_hex_id SET d.duration = $duration"""
    graph.query(q=q, params=params)
    graph.commit()
    print("Graph edge is updated successfully..")


def add_node_edge_to_graph(hex_data, new_duration):
    """
    Add new edge to new destination for existing source node (# TODO if source does not exist?)
    :param hex_data: dict-> contains restaurant hex id and delivery hex id
    :param new_duration: calculated duration in minutes
    :return:
    """
    graph = Graph(
        name="DistGraph",
        redis_con=r
    )
    params = {"source_hex_id": hex_data["Restaurant_hex_id"],
              "destination_hex_id": hex_data["Delivery_hex_id"],
              "duration": new_duration}
    q = """MATCH (s: source) WHERE s.hexId = $source_hex_id CREATE (s)-[d:delivery {duration:$duration}]->(:destination {hexId:$destination_hex_id})"""
    graph.query(q=q, params=params)
    graph.commit()
    print("New component (n or e) added to graph successfully..")


# check_location_pairs_exist_in_graph("8660a259fffffff", "8642ca85fffffff")
