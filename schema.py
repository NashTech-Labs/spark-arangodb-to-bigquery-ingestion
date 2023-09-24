from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

'''BigQuery schema for person_hobbies table'''

hobbies_schema: StructType = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("_key", StringType(), nullable=False),
    StructField("_rev", StringType(), nullable=False),
    StructField("name", StringType()),
    StructField("id", IntegerType()),
    StructField("age", IntegerType()),
    StructField("hobbies", StructType([
        StructField("indoor", ArrayType(StringType())),
        StructField("outdoor", ArrayType(StringType())),
    ])),
    StructField("updatedAt", StringType())
])


