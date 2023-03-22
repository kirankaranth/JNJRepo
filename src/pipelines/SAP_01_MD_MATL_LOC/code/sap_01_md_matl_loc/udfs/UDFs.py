from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)
mandt_code_map = {"bbl" : "100", "p01" : "020", "bwi" : "400"}

def registerUDFs(spark: SparkSession):
    spark.udf.register("get_mandt_code", get_mandt_code)

@udf(returnType = StringType())
def get_mandt_code(mandt: str):
    return mandt_code_map.get(mandt, "Not Found")
