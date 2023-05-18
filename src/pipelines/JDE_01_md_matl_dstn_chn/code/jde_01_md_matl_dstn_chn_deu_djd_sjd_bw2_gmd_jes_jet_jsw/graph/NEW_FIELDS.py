from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.config.ConfigStore import *
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.SRC_SYS_CD))\
        .withColumn("MATL_NUM", col("imlitm"))\
        .withColumn(
          "SL_ORG_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "DSTR_CHNL_CD",
          lit(
            "#"
          )
        )\
        .withColumn("PROD_HIER_CD", trim(col("imsrp1")))\
        .withColumn("DELV_PLNT_CD", lit(None).cast(StringType()))\
        .withColumn("MATL_SLS_CAT_GRP_CD", lit(None).cast(StringType()))\
        .withColumn("ACTG_GRP_CD", lit(None).cast(StringType()))\
        .withColumn("DSTN_CHN_STS_CD", lit(None).cast(StringType()))\
        .withColumn("VLD_FROM_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("ENTRP_DSTN_CHN_STS_CD", lit(None).cast(StringType()))\
        .withColumn("MATL_BASE_CD", lit(None).cast(StringType()))\
        .withColumn("VOL_REBT_GRP", lit(None).cast(StringType()))\
        .withColumn("MATL_PRC_GRP", lit(None).cast(StringType()))\
        .withColumn("MATL_GRP_1", lit(None).cast(StringType()))\
        .withColumn("MATL_GRP_2", trim(col("imprp4")))\
        .withColumn("MATL_GRP_3", trim(col("imprp3")))\
        .withColumn("MATL_GRP_4", trim(col("imsrp4")))\
        .withColumn("MATL_GRP_5", trim(col("imprp5")))\
        .withColumn("PRC_REF_MATL", lit(None).cast(StringType()))\
        .withColumn("MDD_SALEABLE", lit(None).cast(StringType()))\
        .withColumn("PHARMA_SALEABLE", lit(None).cast(StringType()))\
        .withColumn("CNSMR_SALEABLE", lit(None).cast(StringType()))\
        .withColumn("COMMSN_GRP", lit(None).cast(StringType()))\
        .withColumn("RD_PRFL", lit(None).cast(StringType()))\
        .withColumn("CASH_DISC_IN", lit(None).cast(StringType()))\
        .withColumn("MATL_STATS_GRP", lit(None).cast(StringType()))\
        .withColumn("MATL_DSTN_CHN", lit(None).cast(StringType()))\
        .withColumn("DEL_IN", lit(None).cast(StringType()))\
        .withColumn("MIN_ORDR_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("MIN_DELV_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("MIN_MTO_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("DELV_UNIT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("UOM_DELV_UNIT", lit(None).cast(StringType()))\
        .withColumn("SLS_UNIT", lit(None).cast(StringType()))\
        .withColumn("ASRTMNT_GRADE", lit(None).cast(StringType()))\
        .withColumn("EXTRNL_ASRTMNT_PRIR", lit(None).cast(StringType()))\
        .withColumn("LIST_PCDR_STR_ASRTMNT_CAT", lit(None).cast(StringType()))\
        .withColumn("LIST_PCDR_DC_ASRTMNT_CAT", lit(None).cast(StringType()))\
        .withColumn("LIST_FUNC_ASRTMNT_ACT", lit(None).cast(StringType()))\
        .withColumn("STR_LIST_FROM_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("STR_LIST_TO_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("DC_LIST_FROM_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("DC_LIST_TO_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("STR_SOLD_FRM_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("STR_SOLD_TO_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("DC_SOLD_FRM_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("DC_SOLD_TO_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("PROD_ATTR_ID_4", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_5", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_6", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_7", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_8", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_9", lit(None).cast(StringType()))\
        .withColumn("PROD_ATTR_ID_10", lit(None).cast(StringType()))\
        .withColumn("UOM_GRP", lit(None).cast(StringType()))\
        .withColumn("MAX_DELV_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("RACKJOBBER_MATL", lit(None).cast(StringType()))\
        .withColumn("PRC_FIX_IN", lit(None).cast(StringType()))\
        .withColumn("VAR_SLS_UNIT_IN", lit(None).cast(StringType()))\
        .withColumn("MATL_CMPT_CHAR", lit(None).cast(StringType()))\
        .withColumn("MATL_SORT", lit(None).cast(IntegerType()))\
        .withColumn("PRC_BND_CAT", lit(None).cast(StringType()))\
        .withColumn("MATL_SLS_CAT_GRP_DESC", lit(None).cast(StringType()))\
        .withColumn("PROD_HIER_LVL_NUM", lit(None).cast(IntegerType()))\
        .withColumn("PROD_HIER_DESC", lit(None).cast(StringType()))\
        .withColumn("DSTN_CHN_STS_CD_DESC", lit(None).cast(StringType()))\
        .withColumn("BLOK_FOR_SLS_ORDR", lit(None).cast(StringType()))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SL_ORG_NUM', SL_ORG_NUM, 'DSTR_CHNL_CD', DSTR_CHNL_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SL_ORG_NUM', SL_ORG_NUM, 'DSTR_CHNL_CD', DSTR_CHNL_CD)"
              )
            )
          )
        )\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
