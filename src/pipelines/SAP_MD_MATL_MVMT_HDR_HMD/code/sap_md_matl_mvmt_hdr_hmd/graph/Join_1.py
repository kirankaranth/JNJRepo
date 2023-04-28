from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_mvmt_hdr_hmd.config.ConfigStore import *
from sap_md_matl_mvmt_hdr_hmd.udfs.UDFs import *

def Join_1(spark: SparkSession, matdoc: DataFrame, t003t: DataFrame, ) -> DataFrame:
    return matdoc\
        .alias("matdoc")\
        .join(t003t.alias("t003t"), (col("matdoc.blart") == col("t003t.blart")), "left_outer")\
        .select(*[col("ltext")], col("matdoc.*"))
