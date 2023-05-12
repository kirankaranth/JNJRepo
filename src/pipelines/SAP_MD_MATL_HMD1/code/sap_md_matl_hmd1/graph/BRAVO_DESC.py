from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def BRAVO_DESC(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    df1 = spark.sql("SELECT MAT_NUM, in1.ATWTB FROM in1 INNER JOIN in0  ON substr(in1.ATWTB,1,6) = ATWRT")

    return df1
