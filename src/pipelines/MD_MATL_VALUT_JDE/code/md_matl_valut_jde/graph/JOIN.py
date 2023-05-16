from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), ((col("in0.COMCU") == col("in1.LIMCU")) & (col("in0.COITM") == col("in1.LIITM"))), "left_outer")
