"""
Get arrangoDB query to read all documents from a collection.
 :keyword
  - collection -- arangoDB collection to read.
 :return
    str: Query String.
"""


def full_load_query(collection: str):
    return f"""FOR doc IN {collection} RETURN doc"""


"""
Get arrangoDB query to read documents inserted between last ingestion
 time and current time(incremental load). 
 :keyword
  - collection -- arangoDB collection to read.
  - increment_hour -- hour to schedule incremental ingestion.
 :return
    str: Query String
"""


def incremental_load_query(collection: str, increment_hour: str):
    return f"""FOR d IN {collection}
  FILTER DATE_TIMESTAMP(DATE_SUBTRACT(DATE_NOW(),"PT{increment_hour}H")) <= d.updatedAt AND d.updatedAt <= 
  DATE_TIMESTAMP(DATE_NOW()) SORT d.updatedAt ASC RETURN d"""
