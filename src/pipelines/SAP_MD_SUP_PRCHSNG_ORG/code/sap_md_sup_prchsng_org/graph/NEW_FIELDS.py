from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_prchsng_org.config.ConfigStore import *
from sap_md_sup_prchsng_org.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SUP_NUM", col("LIFNR"))\
        .withColumn("PRCHSNG_ORG_NUM", col("EKORG"))\
        .withColumn(
          "CRT_ON_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("PRCH_BLK_IND", trim(col("SPERM")))\
        .withColumn("DEL_IND", trim(col("LOEVM")))\
        .withColumn("CRNCY_CD", trim(col("WAERS")))\
        .withColumn("PMT_TERM_CD", trim(col("ZTERM")))\
        .withColumn("INCOTERM1_CD", trim(col("INCO1")))\
        .withColumn("INCOTERM2_CD", trim(col("INCO2")))\
        .withColumn("PRC_PCDR_CD", trim(col("KALSK")))\
        .withColumn("PRC_CNTL_CD", trim(col("MEPRF")))\
        .withColumn("EVAL_RCPT_SETLM_CD", trim(col("XERSY")))\
        .withColumn("RTRN_VEND_IND", trim(col("KZRET")))\
        .withColumn("CNFRM_CD", trim(col("BSTAE")))\
        .withColumn("NM_OF_PRSN_RESP_CREAT_OBJ", trim(col("ERNAM")))\
        .withColumn("GR_BAS_INVC_VERIF", trim(col("WEBRE")))\
        .withColumn("AUTO_GNR_OF_PO_ALLW", trim(col("KZAUT")))\
        .withColumn("AUTO_EVAL_RCPT_SETLM", trim(col("XERSR")))\
        .withColumn("OWN_EXPLN_OF_TERM_OF_PMT", trim(col("VSBED")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SUP_NUM', SUP_NUM, 'PRCHSNG_ORG_NUM', PRCHSNG_ORG_NUM)")
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SUP_NUM', SUP_NUM, 'PRCHSNG_ORG_NUM', PRCHSNG_ORG_NUM)")
            )
          )
        )\
        .withColumn("_deleted_", col("_deleted_"))
