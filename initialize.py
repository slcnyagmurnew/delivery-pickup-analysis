import polars as pl
from redisgraph import Graph, Node, Edge
import redis
import os


REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))


r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT
)


def construct_graph(data: pl.DataFrame):
    """
    Add source and destination nodes and edges between them, create graph
    :param data: dataframe
    :return:
    """
    unq_src_hex_id_list = data["Restaurant_hex_id"].unique()  # avoid duplication
    for src_hex_id in unq_src_hex_id_list:
        src_node = Node(label="source", properties={"hexId": src_hex_id})
        redis_graph.add_node(src_node)
        destinations = data.filter(pl.col("Restaurant_hex_id") == src_hex_id)
        for row in destinations.rows(named=True):
            dst_node = Node(label="destination", properties={"hexId": row["Delivery_hex_id"]})
            redis_graph.add_node(dst_node)
            redis_graph.add_edge(Edge(src_node=src_node, relation="delivery",
                                      properties={"duration": row["Time_median"]},
                                      dest_node=dst_node))
            """Always duplicated source and destination nodes so not used"""
            # params = {"duration": row["Time_median"], "source_hex_id": row["Restaurant_hex_id"],
            #           "destination_hex_id": row["Delivery_hex_id"]}
            # query = """CREATE (s: source {hexId: $source_hex_id})-
            # [:delivery {duration:$duration}]->(g: destination {hexId: $destination_hex_id})"""
            # redis_graph.query(q=query, params=params, timeout=10)
            # result.pretty_print()  slow...
    print("Redis Graph is constructed with all nodes and their relations..")
    redis_graph.commit()


def group_data_with_sources_and_destinations(data: pl.DataFrame) -> pl.DataFrame:
    """
    Group delivery locations after restaurant locations grouping
    Get median of values as edge weight (delivery time in minutes)
    :param data: dataframe
    :return:
    """
    q = (
        data.lazy()
        .groupby(by=["Restaurant_hex_id", "Delivery_hex_id"])
        .agg(aggs=[pl.median("Time_taken(min)").alias("Time_median")])
        .sort("Restaurant_hex_id")
    )
    dataframe = q.collect()
    print(dataframe.head(n=5))
    print(f"Total size: {dataframe.shape}")
    return dataframe


if __name__ == '__main__':
    redis_graph = Graph(
        name="DistGraph",
        redis_con=r
    )

    df = pl.read_csv("data/processed_data.csv")
    grouped_data = group_data_with_sources_and_destinations(data=df)
    construct_graph(data=grouped_data)
