from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def SORT_VALUES(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql("SELECT XBITM AS T162_XBITM, XB_T162, Plants FROM in0 ORDER BY XBITM, XB_T162, Plants DESC")

    return df1
