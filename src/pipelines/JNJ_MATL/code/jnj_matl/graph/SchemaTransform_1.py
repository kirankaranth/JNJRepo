from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MARA_MATNR"))\
        .withColumn("MATL_TYPE_CD", trim(col("mtart")))\
        .withColumn("FRANCHISE_CD", trim(col("spart")))\
        .withColumn("LCL_PLNG_SUB_FRAN_CD", trim(col("labor")))\
        .withColumn("PRCHSNG_VAL_KEY_CD", trim(col("ekwsl")))\
        .withColumn("DEL_IND", trim(col("lvorm")))\
        .withColumn("MATL_GRP_CD", trim(col("matkl")))\
        .withColumn("INDSTR_SECTR_CD", trim(col("mbrsh")))\
        .withColumn("BASE_UOM_CD", trim(col("meins")))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", col("mhdhb").cast(DecimalType(18, 4)))\
        .withColumn("MIN_SHLF_RMN_LIF_DAYS_CNT", col("mhdrz").cast(DecimalType(18, 4)))\
        .withColumn("MATL_STS_CD", trim(col("mstae")))\
        .withColumn("DSTN_CHN_STS_CD", trim(col("mstav")))\
        .withColumn("NET_WT_MEAS", col("ntgew").cast(DecimalType(18, 4)))\
        .withColumn("PROD_HIER_CD", trim(col("prdha")))\
        .withColumn("PRCMT_QUAL_MGMT_IND", trim(col("qmpur")))\
        .withColumn("STRG_CONDS_CD", trim(col("raube")))\
        .withColumn("LBL_TEMP_RNG", trim(col("tempb")))\
        .withColumn("TRSPN_GRP_CD", trim(col("tragr")))\
        .withColumn("BTCH_MNG_IND", trim(col("xchpf")))\
        .withColumn("MATL_DOC_NUM", trim(col("zeinr")))\
        .withColumn("MATL_DOC_VERS_NUM", trim(col("zeivr")))\
        .withColumn("MATL_SHRT_DESC", trim(col("MAKTX")))\
        .withColumn("MMS_SURGERY_TYPE_CD", trim(col("zzmmssurgtype")))\
        .withColumn("MMS_MATL_TYPE_CD", trim(col("zzmmstype")))\
        .withColumn("PRMRY_PLNT_CD", trim(col("zzwerks")))\
        .withColumn("MMS_FIN_CLSN_CD", trim(col("zzmmsficlass")))\
        .withColumn("MMS_STERILIZATION_IND", trim(col("zzmmsterile")))\
        .withColumn("MATL_CATLG_NUM", trim(col("zzcatnumber")))\
        .withColumn("SRC_SECTR_CD", trim(col("zzsector")))\
        .withColumn("MATL_PARNT_CD", trim(col("zzp2_basecode")))\
        .withColumn("MATL_SUB_TYPE_CD", trim(col("zzmatsub_type")))\
        .withColumn("FIN_HIER_BASE_CD", trim(col("zzsec_prdgrp")))\
        .withColumn("IMPLNT_INSTM_IND", trim(col("zzprod_cat1")))\
        .withColumn("MATL_MOD_CD", trim(col("zzvariant")))\
        .withColumn("KIT_IND", trim(col("zzkit_ind")))\
        .withColumn("MMS_TEMP_SENS_IND", trim(col("zzmmsts")))\
        .withColumn("MATL_CAT_GRP_CD", trim(col("mtpos_mara")))\
        .withColumn("PLNG_HIER3_CD", trim(col("zzp3_low_level")))\
        .withColumn("MATL_SPEC_NUM", when((col("ATNAM") == lit("MATERIAL_SPEC")), col("ATWRT")).otherwise(lit(None)))\
        .withColumn("MATL_SPEC_VERS_NUM", when((col("ATNAM") == lit("SPEC_REV_LEVEL")), col("ATWRT")).otherwise(lit(None)))\
        .withColumn("MATL_TYPE_DESC", lookup("SAP_T134T_LU_JNJ", col("MTART")).getField("MTBEZ"))
