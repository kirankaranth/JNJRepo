from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def SQL(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql("SELECT LIITM, LIMCU AS PLNT_CD, IMLITM, TOT_INV FROM in0 WHERE TOT_INV != 0")

    return df1
