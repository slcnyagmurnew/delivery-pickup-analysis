import polars as pl
from redisgraph import Graph, Node
import redis


r = redis.Redis(
    host="localhost",
    port=6379
)


def construct_graph_with_nodes(data: pl.DataFrame):
    """
    Add source nodes to Graph
    :param data: dataframe
    :return:
    """
    unq_src_hex_id_list = data["Restaurant_hex_id"].unique()  # avoid duplication
    for hex_id in unq_src_hex_id_list:  # row in a dictionary format
        redis_graph.add_node(Node(label="source", properties={"hexId": hex_id}))

    redis_graph.commit()
    print(f"All source nodes` added to Graph with hex ids. \nSize: {len(redis_graph.nodes)}")


def construct_graph_with_edges(data: pl.DataFrame):
    """
    Add edges between source nodes and destination nodes
    :param data: dataframe
    :return:
    """
    for row in data.rows(named=True):
        print(row["Time_median"])
        params = {"duration": row["Time_median"], "source_hex_id": row["Restaurant_hex_id"],
                  "destination_hex_id": row["Delivery_hex_id"]}
        query = """MATCH (s: source) 
                    WHERE s.hexId = $source_hex_id 
                    CREATE (s)-[d:delivery {duration:$duration}]->(:destination {hexId:$destination_hex_id})"""
        redis_graph.query(q=query, params=params, timeout=10)
        # result.pretty_print()  slow...
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
    construct_graph_with_nodes(data=grouped_data)
    construct_graph_with_edges(data=grouped_data)