from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.config.ConfigStore import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.udfs.UDFs import *

def F4102_F4101(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.IBITM") == col("in1.IMITM")), "inner")
