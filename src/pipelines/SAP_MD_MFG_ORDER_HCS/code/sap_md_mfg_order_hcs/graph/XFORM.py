from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_hcs.config.ConfigStore import *
from sap_md_mfg_order_hcs.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("AUART"))\
        .withColumn("MFG_ORDR_NUM", col("AUFNR"))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn(
          "CRTD_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("DEL_IND", trim(col("LOEKZ")))\
        .withColumn("OBJECT_NUMBER", trim(col("OBJNR")))\
        .withColumn("PLNT_CD", trim(col("WERKS")))\
        .withColumn("ORDR_CAT", trim(col("AUTYP")))\
        .withColumn("REF_ORDR_NUM", trim(col("REFNR")))\
        .withColumn("ENT_BY", trim(col("ERNAM")))\
        .withColumn("LAST_CHG_BY", trim(col("AENAM")))\
        .withColumn("DESC", trim(col("KTEXT")))\
        .withColumn("LONG_TEXT_EXISTS", trim(col("LTEXT")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("BUSN_AREA", trim(col("GSBER")))\
        .withColumn("CNTL_AREA", trim(col("KOKRS")))\
        .withColumn("COST_CLCT_KEY", trim(col("CCKEY")))\
        .withColumn("ORDR_CRNCY", trim(col("WAERS")))\
        .withColumn("ORDR_STS", trim(col("ASTNR")))\
        .withColumn(
          "LAST_STS_CHG_DTTM",
          when((col("STDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("STDAT"), "yyyyMMdd"))
        )\
        .withColumn("STS_REACHED_SO_FAR", trim(col("ESTNR")))\
        .withColumn("PH_ORDR_RELS", trim(col("PHAS1")))\
        .withColumn("PH_ORDR_CMPLT", trim(col("PHAS2")))\
        .withColumn(
          "PLAN_CMPLT_DTTM",
          when((col("PDAT2") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("PDAT2"), "yyyyMMdd"))
        )\
        .withColumn(
          "PLAN_CLS_DTTM",
          when((col("PDAT3") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("PDAT3"), "yyyyMMdd"))
        )\
        .withColumn(
          "RLSE_DTTM",
          when((col("IDAT1") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT1"), "yyyyMMdd"))
        )\
        .withColumn(
          "TECH_CMPLT_DTTM",
          when((col("IDAT2") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT2"), "yyyyMMdd"))
        )\
        .withColumn(
          "CLSE_DTTM",
          when((col("IDAT3") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT3"), "yyyyMMdd"))
        )\
        .withColumn("OBJ_ID", trim(col("OBJID")))\
        .withColumn("USG_OF_THE_COND_TBL", trim(col("KVEWE")))\
        .withColumn("APPL", trim(col("KAPPL")))\
        .withColumn("COST_SHT", trim(col("KALSM")))\
        .withColumn("OVHD_KEY", trim(col("ZSCHL")))\
        .withColumn("PRCSG_GRP", trim(col("ABKRS")))\
        .withColumn("SEQ_NUM", trim(col("SEQNR")))\
        .withColumn("APPLT", trim(col("USER0")))\
        .withColumn("EST_TOT_COSTS_OF_ORDR", col("USER4").cast(DecimalType(18, 4)))\
        .withColumn(
          "APPL_DTTM",
          when((col("USER5") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER5"), "yyyyMMdd"))
        )\
        .withColumn(
          "WRK_STRT_DTTM",
          when((col("USER7") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER7"), "yyyyMMdd"))
        )\
        .withColumn(
          "END_OF_WRK_DTTM",
          when((col("USER8") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER8"), "yyyyMMdd"))
        )\
        .withColumn("PRFT_CTR", trim(col("PRCTR")))\
        .withColumn("WRK_BRKDWN_STRC_ELMNT", trim(col("PSPEL")))\
        .withColumn("VAR_KEY", trim(col("AWSLS")))\
        .withColumn("RSLTS_ANAL_KEY", trim(col("ABGSL")))\
        .withColumn("REQ_CO_CD", trim(col("ABUKR")))\
        .withColumn("ITM_NUM_IN_SLS_ORDR", trim(col("KDPOS")))\
        .withColumn("PRDTN_PRCS", trim(col("PROCNR")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("L0_DELETED", col("_deleted_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "PRIM_KEY",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
            )
          )
        )\
        .withColumn("_pk_md5_", md5(
        to_json(
          expr(
            "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
          )
        )
    ))
