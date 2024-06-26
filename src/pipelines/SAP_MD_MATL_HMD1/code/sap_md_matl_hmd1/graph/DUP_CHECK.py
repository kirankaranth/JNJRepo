from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def DUP_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MATL_NUM"))

    return df1.agg(count(lit(1)).alias("Records"))
