from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("BOM_CAT_CD", col("ixtbm"))\
        .withColumn(
          "BOM_NUM",
          concat(
            col("ixkit"), 
            lit(";"), 
            col("ixmmcu"), 
            lit(";"), 
            col("ixtbm"), 
            lit(";"), 
            col("ixbqty"), 
            lit(";"), 
            col("ixcoby"), 
            lit(";"), 
            col("ixcpnt"), 
            lit(";"), 
            col("ixsbnt")
          )
        )\
        .withColumn("ALT_BOM_NUM", lit("01").cast(StringType()))\
        .withColumn("BOM_CNTR_NBR", concat(col("ixcpnt"), lit("."), col("ixsbnt")))\
        .withColumn(
          "BOM_VLD_FROM_DTTM",
          when(
              (
                ((lower(trim(col("ixefff"))) == lit("null")) | (trim(col("ixefff")) == lit("")))
                | (trim(col("ixefff")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ixefff")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ixefff")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
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
        .withColumn("CHG_NUM", trim(col("ixrvno")))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                ((lower(trim(col("ixefff"))) == lit("null")) | (trim(col("ixefff")) == lit("")))
                | (trim(col("ixefff")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ixefff")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ixefff")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
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
        .withColumn(
          "CHG_DTTM",
          when(
              (
                ((lower(trim(col("ixupmj"))) == lit("null")) | (trim(col("ixupmj")) == lit("")))
                | (trim(col("ixupmj")) == lit("0"))
              ), 
              lit(None).cast(TimestampType())
            )\
            .otherwise(to_timestamp(
            concat(
              substring(
                date_add(
                    concat(
                      substring((trim(col("ixupmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                      lit("-01-01")
                    ), 
                    (
                      substring((trim(col("ixupmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                        .cast(IntegerType())
                      - 1
                    )
                  )\
                  .cast(StringType()), 
                1, 
                10
              ), 
              lit(" "), 
              lpad(trim(col("ixtday")), 6, "0")
            ), 
            "yyyy-MM-dd HHmmss"
          ))
        )\
        .withColumn("BOM_TXT", trim(col("ixdsc1")))\
        .withColumn(
          "BOM_VLD_TO_DTTM",
          when(
              (
                ((lower(trim(col("ixefft"))) == lit("null")) | (trim(col("ixefft")) == lit("")))
                | (trim(col("ixefft")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ixefft")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ixefft")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
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
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
