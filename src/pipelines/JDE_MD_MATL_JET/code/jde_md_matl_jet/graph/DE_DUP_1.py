from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def DE_DUP_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("row_number", row_number().over(Window.partitionBy("XBITM").orderBy(lit(1))))\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
