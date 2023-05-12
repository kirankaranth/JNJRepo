from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))\
        .withColumn("MATL_NUM", col("COLITM"))\
        .withColumn("VALUT_AREA_CD", col("COMCU"))\
        .withColumn("PRC_CNTL_IND", lit("07"))\
        .withColumn("TOT_STK_QTY", lookup("INV_LU", col("COITM"), col("COMCU")).getField("TOT_INV"))\
        .withColumn("PRC_AMT", col("COUNCS").cast(DecimalType(18, 4)))
