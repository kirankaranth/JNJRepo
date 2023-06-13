from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_inv_hm2.config.ConfigStore import *
from sap_01_md_matl_inv_hm2.udfs.UDFs import *

def SetOperation_1_Union(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0.unionAll(in1).unionAll(in2)
