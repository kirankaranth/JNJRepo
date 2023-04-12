from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("MATL_TYPE_CD", trim(col("MTART")))\
        .withColumn("BRND_CD", trim(col("BRAND_ID")))\
        .withColumn("FRANCHISE_CD", trim(col("SPART")))\
        .withColumn("LCL_PLNG_SUB_FRAN_CD", trim(col("LABOR")))\
        .withColumn("PRCHSNG_VAL_KEY_CD", trim(col("EKWSL")))\
        .withColumn("DEL_IND", trim(col("LVORM")))\
        .withColumn("MATL_GRP_CD", trim(col("MATKL")))\
        .withColumn("INDSTR_SECTR_CD", trim(col("MBRSH")))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", trim(col("MHDHB")))\
        .withColumn("MIN_SHLF_RMN_LIF_DAYS_CNT", trim(col("MHDRZ")))\
        .withColumn("MATL_STS_CD", trim(col("MSTAE")))\
        .withColumn("DSTN_CHN_STS_CD", trim(col("MSTAV")))\
        .withColumn("NET_WT_MEAS", trim(col("NTGEW")))\
        .withColumn("PROD_HIER_CD", trim(col("PRDHA")))\
        .withColumn("PRCMT_QUAL_MGMT_IND", trim(col("QMPUR")))\
        .withColumn("STRG_CONDS_CD", trim(col("RAUBE")))\
        .withColumn("LBL_TEMP_RNG\t", trim(col("TEMPB")))\
        .withColumn("TRSPN_GRP_CD", trim(col("TRAGR")))
