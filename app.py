# import uvicorn
from fastapi import FastAPI, Request, status
from graphops import predict_delivery, update_edge_of_graph, add_node_edge_to_graph

app = FastAPI()


@app.get("/")
async def root():
    return {"msg": "Hi from first FastApi!", "status": 200}


@app.post("/update/")
async def update(request_body: dict):
    """
    Update Graph
    Update edge value or add new edge to nodes
    :param request_body:
        {"hex_data": {"Restaurant_hex_id": "86603386fffffff", "Delivery_hex_id": "8642ca85fffffff"},
         "exists": true/false (returned from 'predict' request),
         "predicted_duration": 34.5 (returned from 'predict' request)}
    :return:
    """
    exists = request_body.get("exists")
    hex_data = request_body.get("hex_data")
    predicted_duration = request_body.get("predicted_duration")

    if exists:
        update_edge_of_graph(hex_data=hex_data, new_duration=predicted_duration)
        return {"msg": "Edge value updated", "status": 200}
    else:
        add_node_edge_to_graph(hex_data=hex_data, new_duration=predicted_duration)
        return {"msg": "New edge added to Graph", "status": 200}


@app.post("/predict/")
async def predict(hex_data: dict, data: dict, status_code=status.HTTP_200_OK):
    """
    Predict delivery duration for existing locations or estimate with CatBoost model for non-connected nodes
        - Option 1: Get route duration from OSRM. Update edge value with the mean of OSRM result and existing edge duration
        - Option 2: Load CatBoost Regressor model (obtained data is required). Create new edge between existing nodes
    :param hex_data: dict -> {"Restaurant_hex_id": "86603386fffffff", "Delivery_hex_id": "8642ca85fffffff"}
    :param data: dict -> conditions for delivery
        {"Road_traffic_density": "Medium", "Type_of_vehicle": "motorcycle",
        "Distance(m)": 20253.808188033847,
        "Weatherconditions": "conditions Sandstorms",
        "Vehicle_condition": 0, "multiple_deliveries": 2}
    :param status_code:
    :return:
    """
    predicted_duration, exists = predict_delivery(hex_data=hex_data, data=data)
    return {"duration": predicted_duration, "exist": exists, "status": status_code}


# if __name__ == '__main__':
#     uvicorn.run(app, port=3000, host="0.0.0.0", reload=True)
