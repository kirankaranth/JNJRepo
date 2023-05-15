from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", coalesce(lookup("F4101_LU", col("COITM")).getField("IMLITM"), col("COITM")))\
        .withColumn("VALUT_AREA_CD", col("COMCU"))\
        .withColumn("PRC_CNTL_IND", lit("07"))\
        .withColumn("VALUT_TYPE_CD", col("LIPBIN"))\
        .withColumn("TOT_STK_QTY", (col("TOT_INV") / lit(Config.divisor)).cast(DecimalType(18, 4)))\
        .withColumn("VALUT_CLS_CD", trim(lookup("F4101_LU", col("LIITM")).getField("IMGLPT")))\
        .withColumn("BASE_UOM_CD", trim(lookup("F4101_LU", col("LIITM")).getField("IMUOM1")))\
        .withColumn("PRC_AMT", col("COUNCS").cast(DecimalType(18, 4)))\
        .withColumn("PRC_UNIT_NBR", lit(10000))
