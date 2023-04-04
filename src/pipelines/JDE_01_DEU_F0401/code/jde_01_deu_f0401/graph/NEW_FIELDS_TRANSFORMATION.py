from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumnRenamed("A6AN8", "SUP_NUM")\
        .withColumn(
          "PRCHSNG_ORG_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "CRT_ON_DTTM",
          when(
              (
                ((lower(trim(col("a6upmj"))) == lit("null")) | (trim(col("a6upmj")) == lit("")))
                | (trim(col("a6upmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("a6upmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("a6upmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 3)\
                      .cast(IntegerType())
                    - 1
                  )
                )\
                .cast(StringType()), 
              1, 
              10
            ), 
            "yyyy-MM-dd"
          ))
        )\
        .withColumn("PRCH_BLK_IND", lit(None))\
        .withColumn("DEL_IND", lit(None))\
        .withColumn("CRNCY_CD", trim(col("A6CRRP")))\
        .withColumn("PMT_TERM_CD", lit(None))\
        .withColumn("INCOTERM1_CD", trim(col("A6FRTH")))\
        .withColumn("INCOTERM2_CD", col("F0101_ABMCU"))\
        .withColumn("PRC_PCDR_CD", lit(None))\
        .withColumn("PRC_CNTL_CD", lit(None))\
        .withColumn("EVAL_RCPT_SETLM_CD", trim(col("A6AVCH")))\
        .withColumn("RTRN_VEND_IND", lit(None))\
        .withColumn("CNFRM_CD", lit(None))\
        .withColumn("NM_OF_PRSN_RESP_CREAT_OBJ", lit(None))\
        .withColumn("GR_BAS_INVC_VERIF", lit(None))\
        .withColumn("AUTO_GNR_OF_PO_ALLW", lit(None))\
        .withColumn("AUTO_EVAL_RCPT_SETLM", lit(None))\
        .withColumn("OWN_EXPLN_OF_TERM_OF_PMT", lit(None))\
        .withColumn("SHIPPING_COND_CD", lit(None))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
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
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
