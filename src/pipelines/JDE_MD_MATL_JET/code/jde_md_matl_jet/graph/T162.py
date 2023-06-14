from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def T162(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("XBITM"), col("XB_T162"))

    return df1.agg(count(col("XBMCU")).alias("Plants"))
