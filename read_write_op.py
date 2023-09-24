from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def read_write(spark: SparkSession, arango_connection: Dict[str, str],
               bq_table: str, bq_dataset: str, bq_project: str,
               query: str, doc_schema: StructType):

    print(f"spark read arango collection")
    df: DataFrame = spark.read.format("com.arangodb.spark") \
        .option("query", query) \
        .option("batchSize", 2147483647) \
        .options(**arango_connection) \
        .schema(doc_schema).load()

    # ADD the Transformation steps before writing to bigquery

    print(f"spark writing to big query {bq_table} table")
    df.write.format('bigquery').mode("append") \
        .option('table', bq_table) \
        .option("project", bq_project) \
        .option("dataset", bq_dataset) \
        .option("writeMethod", "direct") \
        .option('credentialsFile', 'key.json') \
        .save()
