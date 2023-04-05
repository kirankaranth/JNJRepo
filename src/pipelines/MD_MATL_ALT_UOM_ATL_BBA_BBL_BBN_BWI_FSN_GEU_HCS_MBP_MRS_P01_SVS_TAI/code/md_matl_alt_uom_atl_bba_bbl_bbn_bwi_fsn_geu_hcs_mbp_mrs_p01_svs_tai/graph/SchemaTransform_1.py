from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("ALT_UOM_CD", col("MEINH"))\
        .withColumn("BASE_UOM_CD", trim(lookup("LU_MARA_MEINS", col("MATNR")).getField("MEINS")))\
        .withColumn("LCL_FACT_NUMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("LCL_FACT_DENOM_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("FACT_NUMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("FACT_DENOM_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("GTIN_NUM", trim(col("EAN11")))\
        .withColumn("GTIN_CAT_CD", trim(col("NUMTP")))\
        .withColumn("WDTH_MEAS", col("BREIT").cast(DecimalType(18, 4)))\
        .withColumn("DIM_UOM_CD", trim(col("MEABM")))
