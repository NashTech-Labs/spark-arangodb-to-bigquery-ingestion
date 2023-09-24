from pyspark.sql import SparkSession
from argparse import ArgumentParser
import ingestionjob

"""
create a spark session an entry point of spark application.

  Keyword Arguments:
      - master -- where to run locally or on a spark cluster.
      - gcp_project_id -- google cloud project id to connect to Big Query.

  :return
    SparkSession: spark session    
"""


def create_spark_session(master: str, gcp_project_id: str) -> SparkSession:
    return SparkSession.builder \
        .appName("ArangoToBqIngestion") \
        .master(master) \
        .config("spark.jars.packages",
                'com.arangodb:arangodb-spark-datasource-3.3_2.12:1.5.0,'
                'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1') \
        .config('parentProject', gcp_project_id) \
        .getOrCreate()


def main():
    parser = ArgumentParser()
    parser.add_argument("--arangoPassword", default="rootpassword")
    parser.add_argument("--arangoEndpoints", default="localhost:8529")
    parser.add_argument("--arangoDatabase")
    parser.add_argument("--arangoCollection")
    parser.add_argument("--bqProjectId")
    parser.add_argument("--bqDataSet")
    parser.add_argument("--bqTargetTable")
    parser.add_argument("--fullLoadOrIncremental", default="full_load")
    parser.add_argument("--incrementLoadDeltaTime", default="2")
    parser.add_argument("--master", default="local[*]")

    args = parser.parse_args()

    spark_session = create_spark_session(args.master, args.bqProjectId)

    arango = {
        "endpoints": args.arangoEndpoints,
        "password": args.arangoPassword,
        "database": args.arangoDatabase,
    }

    if args.fullLoadOrIncremental == "full_load":
        # Full load ingestion
        print("full load")
        ingestionjob.full_load_ingestion(spark_session, arango, args)
    elif args.fullLoadOrIncremental == "incremental":
        # incremental load ingestion
        print("incremental load")
        ingestionjob.incremental_load_ingestion(spark_session, arango,
                                                args, args.incrementLoadDeltaTime)
    else:
        print("wrong job type")


if __name__ == "__main__":
    main()
