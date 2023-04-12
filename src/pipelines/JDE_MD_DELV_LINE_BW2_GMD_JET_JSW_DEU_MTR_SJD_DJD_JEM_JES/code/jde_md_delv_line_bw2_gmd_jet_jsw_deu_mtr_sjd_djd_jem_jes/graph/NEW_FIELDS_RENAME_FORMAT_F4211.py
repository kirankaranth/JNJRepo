from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT_F4211(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("DELV_NUM", col("SDDOC"))\
        .withColumn("DELV_LINE_NBR", col("SDLNID"))\
        .withColumn("DELV_TYP_CD", col("SDDCTO"))\
        .withColumn("CO_CD", col("SDKCOO"))\
        .withColumn("DOC_REF_NUM", col("SDDOCO"))\
        .withColumn(
          "ACTL_GI_DTTM",
          when(
              (
                (lower(trim(col("SDADDJ"))).isNull() | (trim(col("SDADDJ")) == lit("")))
                | (trim(col("SDADDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("SDADDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDADDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("ACTL_SLS_UNIT_DELV_QTY", trim(col("SDSOQS")).cast(DecimalType(18, 4)))\
        .withColumn("BTCH_NUM", trim(col("SDLOTN")))\
        .withColumn("MATL_NUM", trim(col("SDLITM")))\
        .withColumn("SLS_ORDR_LINE_NBR_REF", trim(col("SDLNID")))\
        .withColumn("SLS_UOM_CD", trim(col("SDUOM")))\
        .withColumn("SHIPPING_PLNT_CD", trim(col("SDMCU")))\
        .withColumn("SLOC_CD", trim(col("SDLOCN")))\
        .withColumn("CRT_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("ORDR_SFX", col("SDSFXO"))\
        .withColumn(
          "MATCH_TYPE",
          lit(
            "#"
          )
        )\
        .withColumn("LINE_TYPE_DELV", trim(col("SDLNTY")))\
        .withColumn("SRC_TBL_NM", lit(Config.sourceTable1))\
        .withColumn("ORIG_QTY_DELV_ITM", lit(None).cast(DecimalType(18, 4)))\
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
