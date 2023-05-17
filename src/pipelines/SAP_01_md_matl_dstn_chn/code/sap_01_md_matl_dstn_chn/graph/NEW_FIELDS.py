from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn.config.ConfigStore import *
from sap_01_md_matl_dstn_chn.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.SRC_SYS_CD))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("SL_ORG_NUM", col("VKORG"))\
        .withColumn("DSTR_CHNL_CD", col("VTWEG"))\
        .withColumn("PROD_HIER_CD", trim(col("PRODH")))\
        .withColumn("DELV_PLNT_CD", trim(col("DWERK")))\
        .withColumn("MATL_SLS_CAT_GRP_CD", trim(col("MTPOS")))\
        .withColumn("ACTG_GRP_CD", trim(col("KTGRM")))\
        .withColumn("DSTN_CHN_STS_CD", trim(col("VMSTA")))\
        .withColumn(
          "VLD_FROM_DTTM",
          when((col("VMSTD") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VMSTD"), "yyyyMMdd"))
        )\
        .withColumn("ENTRP_DSTN_CHN_STS_CD", lit(Config.ENTRP_DSTN_CHN_STS_CD))\
        .withColumn("MATL_BASE_CD", lit(Config.MATL_BASE_CD))\
        .withColumn("VOL_REBT_GRP", trim(col("BONUS")))\
        .withColumn("MATL_PRC_GRP", trim(col("KONDM")))\
        .withColumn("MATL_GRP_1", trim(col("MVGR1")))\
        .withColumn("MATL_GRP_2", trim(col("MVGR2")))\
        .withColumn("MATL_GRP_3", trim(col("MVGR3")))\
        .withColumn("MATL_GRP_4", trim(col("MVGR4")))\
        .withColumn("MATL_GRP_5", trim(col("MVGR5")))\
        .withColumn("PRC_REF_MATL", trim(col("PMATN")))\
        .withColumn("MDD_SALEABLE", trim(col("PRAT1")))\
        .withColumn("PHARMA_SALEABLE", trim(col("PRAT2")))\
        .withColumn("CNSMR_SALEABLE", trim(col("PRAT3")))\
        .withColumn("COMMSN_GRP", trim(col("PROVG")))\
        .withColumn("RD_PRFL", trim(col("RDPRF")))\
        .withColumn("CASH_DISC_IN", trim(col("SKTOF")))\
        .withColumn("MATL_STATS_GRP", trim(col("VERSG")))\
        .withColumn("MATL_DSTN_CHN", trim(col("VTWEG")))\
        .withColumn("DEL_IN", trim(col("LVORM")))\
        .withColumn("MIN_ORDR_QTY", trim(col("AUMNG")))\
        .withColumn("MIN_DELV_QTY", trim(col("LFMNG")))\
        .withColumn("MIN_MTO_QTY", trim(col("EFMNG")))\
        .withColumn("DELV_UNIT", trim(col("SCMNG")))\
        .withColumn("UOM_DELV_UNIT", trim(col("SCHME")))\
        .withColumn("SLS_UNIT", trim(col("VRKME")))\
        .withColumn("ASRTMNT_GRADE", trim(col("SSTUF")))\
        .withColumn("EXTRNL_ASRTMNT_PRIR", trim(col("PFLKS")))\
        .withColumn("LIST_PCDR_STR_ASRTMNT_CAT", trim(col("LSTFL")))\
        .withColumn("LIST_PCDR_DC_ASRTMNT_CAT", trim(col("LSTVZ")))\
        .withColumn("LIST_FUNC_ASRTMNT_ACT", trim(col("LSTAK")))\
        .withColumn(
          "STR_LIST_FROM_DTTM",
          when((col("LDVFL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("LDVFL"), "yyyyMMdd"))
        )\
        .withColumn(
          "STR_LIST_TO_DTTM",
          when((col("LDBFL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("LDBFL"), "yyyyMMdd"))
        )\
        .withColumn(
          "DC_LIST_FROM_DTTM",
          when((col("LDVZL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("LDVZL"), "yyyyMMdd"))
        )\
        .withColumn(
          "DC_LIST_TO_DTTM",
          when((col("LDBZL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("LDBZL"), "yyyyMMdd"))
        )\
        .withColumn(
          "STR_SOLD_FRM_DTTM",
          when((col("VDVFL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VDVFL"), "yyyyMMdd"))
        )\
        .withColumn(
          "STR_SOLD_TO_DTTM",
          when((col("VDBFL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VDBFL"), "yyyyMMdd"))
        )\
        .withColumn(
          "DC_SOLD_FRM_DTTM",
          when((col("VDVZL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VDVZL"), "yyyyMMdd"))
        )\
        .withColumn(
          "DC_SOLD_TO_DTTM",
          when((col("VDBZL") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VDBZL"), "yyyyMMdd"))
        )\
        .withColumn("PROD_ATTR_ID_4", trim(col("PRAT4")))\
        .withColumn("PROD_ATTR_ID_5", trim(col("PRAT5")))\
        .withColumn("PROD_ATTR_ID_6", trim(col("PRAT6")))\
        .withColumn("PROD_ATTR_ID_7", trim(col("PRAT7")))\
        .withColumn("PROD_ATTR_ID_8", trim(col("PRAT8")))\
        .withColumn("PROD_ATTR_ID_9", trim(col("PRAT9")))\
        .withColumn("PROD_ATTR_ID_10", trim(col("PRATA")))\
        .withColumn("UOM_GRP", trim(col("MEGRU")))\
        .withColumn("MAX_DELV_QTY", trim(col("LFMAX")))\
        .withColumn("RACKJOBBER_MATL", trim(col("RJART")))\
        .withColumn("PRC_FIX_IN", trim(col("PBIND")))\
        .withColumn("VAR_SLS_UNIT_IN", trim(col("VAVME")))\
        .withColumn("MATL_CMPT_CHAR", trim(col("MATKC")))\
        .withColumn("MATL_SORT", trim(col("PVMSO")))\
        .withColumn("PRC_BND_CAT", lit(Config.PRC_BND_CAT))\
        .withColumn("MATL_SLS_CAT_GRP_DESC", lit(Config.MATL_SLS_CAT_GRP_DESC))\
        .withColumn("PROD_HIER_LVL_NUM", lit(Config.PROD_HIER_LVL_NUM).cast(IntegerType()))\
        .withColumn("PROD_HIER_DESC", lit(Config.PROD_HIER_DESC))\
        .withColumn("DSTN_CHN_STS_CD_DESC", lit(Config.DSTN_CHN_STS_CD_DESC))\
        .withColumn("BLOK_FOR_SLS_ORDR", lit(Config.BLOK_FOR_SLS_ORDR))\
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
