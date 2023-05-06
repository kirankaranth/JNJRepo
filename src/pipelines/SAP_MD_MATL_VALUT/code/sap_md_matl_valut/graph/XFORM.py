from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut.config.ConfigStore import *
from sap_md_matl_valut.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.SRC_SYS_CD))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("VALUT_AREA_CD", col("BWKEY"))\
        .withColumn("VALUT_TYPE_CD", col("BWTAR"))\
        .withColumn("PRC_CNTL_IND", trim(col("VPRSV")))\
        .withColumn("TOT_STK_QTY", col("VPRSV").cast(DecimalType(18, 4)))\
        .withColumn("TOT_VAL_AMT", col("VPRSV").cast(DecimalType(18, 4)))\
        .withColumn("MVG_AVG_PRC_AMT", col("VPRSV").cast(DecimalType(18, 4)))\
        .withColumn("PRC_AMT", col("VPRSV").cast(DecimalType(18, 4)))\
        .withColumn("PRC_UNIT_NBR", col("VPRSV").cast(DecimalType(18, 4)))\
        .withColumn("VALUT_CLS_CD", trim(col("BKLAS")))\
        .withColumn("BASE_UOM_CD", trim(lookup("MEINS_LU", col("MATNR")).getField("MEINS")))\
        .withColumn("DEL_FL", trim(col("LVORM")))\
        .withColumn("MATL_USG", trim(col("MTUSE")))\
        .withColumn("MATL_ORIG", trim(col("MTORG")))\
        .withColumn("INHS_PRODCD", trim(col("OWNPR")))\
        .withColumn("CST_ELMNT_ORIG_GRP", trim(col("HRKFT")))\
        .withColumn("COST_OVHD_GRP", trim(col("KOSGR")))\
        .withColumn("TMST", expr(Config.TMST).cast(TimestampType()))
