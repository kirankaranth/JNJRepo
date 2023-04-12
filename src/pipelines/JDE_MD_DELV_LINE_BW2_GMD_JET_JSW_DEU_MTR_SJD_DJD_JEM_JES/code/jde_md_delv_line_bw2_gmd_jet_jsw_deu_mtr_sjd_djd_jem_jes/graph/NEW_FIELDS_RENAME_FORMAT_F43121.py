from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT_F43121(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("DELV_NUM", col("PRDOC"))\
        .withColumn("DELV_LINE_NBR", concat(col("PRLNID"), col("PRNLIN")))\
        .withColumn("DELV_TYP_CD", col("PRDCTO"))\
        .withColumn("CO_CD", col("PRKCOO"))\
        .withColumn("DOC_REF_NUM", col("PRDOCO"))\
        .withColumn(
          "ACTL_GI_DTTM",
          when(
              (
                (lower(trim(col("PRRCDJ"))).isNull() | (trim(col("PRRCDJ")) == lit("")))
                | (trim(col("PRRCDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("PRRCDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("PRRCDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("ACTL_SLS_UNIT_DELV_QTY", trim(col("PRUREC")).cast(DecimalType(18, 4)))\
        .withColumn("BTCH_NUM", trim(col("PRLOTN")))\
        .withColumn("MATL_NUM", trim(col("PRLITM")))\
        .withColumn("SLS_ORDR_LINE_NBR_REF", trim(col("PRLNID")))\
        .withColumn("SLS_UOM_CD", trim(col("PRUOM")))\
        .withColumn("SHIPPING_PLNT_CD", trim(col("PRMCU")))\
        .withColumn("SLOC_CD", trim(col("PRLOCN")))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                (lower(trim(col("PRUPMJ"))).isNull() | (trim(col("PRUPMJ")) == lit("")))
                | (trim(col("PRUPMJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            concat(
              substring(
                date_add(
                    concat(
                      substring((trim(col("PRUPMJ")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                      lit("-01-01")
                    ), 
                    (
                      substring((trim(col("PRUPMJ")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                        .cast(IntegerType())
                      - 1
                    )
                  )\
                  .cast(StringType()), 
                1, 
                10
              ), 
              lit(" "), 
              lpad(trim(col("PRTDAY")), 6, "0")
            ), 
            "yyyy-MM-dd HHmmss"
          ))
        )\
        .withColumn("ORDR_SFX", col("PRSFXO"))\
        .withColumn("MATCH_TYPE", col("PRMATC"))\
        .withColumn("LINE_TYPE_DELV", trim(col("PRLNTY")))\
        .withColumn("SRC_TBL_NM", lit("F43121"))\
        .withColumn("ORIG_QTY_DELV_ITM", trim(col("PRUORG")).cast(DecimalType(18, 4)))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_10_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'DELV_NUM', DELV_NUM, 'DELV_LINE_NBR', DELV_LINE_NBR, 'DELV_TYP_CD', DELV_TYP_CD, 'CO_CD', CO_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'ORDR_SFX', ORDR_SFX, 'MATCH_TYPE', MATCH_TYPE, 'SRC_TBL_NM', SRC_TBL_NM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'DELV_NUM', DELV_NUM, 'DELV_LINE_NBR', DELV_LINE_NBR, 'DELV_TYP_CD', DELV_TYP_CD, 'CO_CD', CO_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'ORDR_SFX', ORDR_SFX, 'MATCH_TYPE', MATCH_TYPE, 'SRC_TBL_NM', SRC_TBL_NM)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))
