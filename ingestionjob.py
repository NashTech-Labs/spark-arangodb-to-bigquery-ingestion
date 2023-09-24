from argparse import Namespace
from typing import Dict
import arango_queries
import read_write_op
import schema
from pyspark.sql import SparkSession

"""
Execute spark job to read all arangodb collection documents and 
writing to Big query table.
 :keyword
  - spark_session -- SparkSession.
  - arango -- arangoDB configuration(host, password, database)
  - args -- Arguments
"""


def full_load_ingestion(spark_session: SparkSession, arango: Dict[str, str], args: Namespace):
    # reading arango hobbies Collection and writing to bigquery hobbies Table
    read_write_op.read_write(spark_session, arango,
                             args.bqTargetTable, args.bqDataSet,
                             args.bqProjectId,
                             arango_queries.full_load_query(args.arangoCollection),
                             schema.hobbies_schema)
    print(f"full load ingestion done {args.bqTargetTable} Table")


"""
Execute spark job to read arangodb collection documents for an incremental hour and 
writing to Big query table.
 :keyword
  - spark_session -- SparkSession.
  - arango -- arangoDB configuration(host, password, database)
  - args -- Arguments
  - increment_hr --  hour to schedule incremental ingestion.
"""


def incremental_load_ingestion(spark_session: SparkSession,
                               arango: Dict[str, str],
                               args: Namespace,
                               increment_hr: str):
    # reading arango hobbies Collection and writing to bigquery hobbies Table
    read_write_op.read_write(spark_session, arango,
                             args.bqTargetTable, args.bqDataSet,
                             args.bqProjectId,
                             arango_queries.incremental_load_query(
                                 args.arangoCollection, increment_hr),
                             schema.hobbies_schema)
    print(f"incremental load ingestion done {args.bqTargetTable} Table")
