from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.udfs.UDFs import *

def NEW_FIELDS_PK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn("VAR_CNTL_CD", trim(col("AWSLS")))\
        .withColumn("AUTO_PO_IND", trim(col("KAUTB")))\
        .withColumn("PRCTR_CD", trim(col("PRCTR")))\
        .withColumn("SRC_LIST_IND", trim(col("KORDB")))\
        .withColumn("LD_GRP_CD", trim(col("LADGR")))\
        .withColumn("QUAL_MGMT_CNTL_CD", trim(col("SSQSS")))\
        .withColumn("REORDR_QTY", col("MINBE").cast(DecimalType(18, 4)))\
        .withColumn("COST_LOT_SIZE_VAL", col("LOSGR").cast(DecimalType(18, 4)))\
        .withColumn("SERIZATION_PRFL_CD", trim(col("SERNP")))\
        .withColumn("SPEC_MATL_PLNT_STS_CD", trim(col("MMSTA")))\
        .withColumn(
          "SPEC_MATL_PLNT_STS_DTTM",
          when((col("MMSTD") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("MMSTD"), "yyyyMMdd"))
        )\
        .withColumn("DEL_IND", trim(col("LVORM")))\
        .withColumn("LOT_SIZE_VAL", trim(col("DISLS")))\
        .withColumn("MATL_PLNR_NUM", trim(col("DISPO")))\
        .withColumn("PRDTN_SUPR_CD", trim(col("FEVOR")))\
        .withColumn("PRDTN_UOM_CD", trim(col("FRTME")))\
        .withColumn("MATL_ABC_CLSN_CD", trim(col("MAABC")))\
        .withColumn("AVLBLTY_CHK_IND", trim(col("MTVFP")))\
        .withColumn("SFTY_TIME_IND", trim(col("SHFLG")))\
        .withColumn("SPL_PRCMT_TYPE_CD", trim(col("SOBSL")))\
        .withColumn("PRCMT_TYPE_CD", trim(col("BESKZ")))\
        .withColumn("MRP_TYPE_CD", trim(col("DISMM")))\
        .withColumn("ORIG_CTRY_CD", trim(col("HERKL")))\
        .withColumn("PLNG_STRTGY_GRP_CD", trim(col("STRGR")))
