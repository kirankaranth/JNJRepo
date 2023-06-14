from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.config.ConfigStore import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.udfs.UDFs import *

def DUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("row_number", row_number().over(Window.partitionBy("SRC_SYS_CD", "MATL_NUM", "PLNT_CD").orderBy(lit(1))))\
        .withColumn("count", count("*").over(Window.partitionBy("SRC_SYS_CD", "MATL_NUM", "PLNT_CD")))\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
