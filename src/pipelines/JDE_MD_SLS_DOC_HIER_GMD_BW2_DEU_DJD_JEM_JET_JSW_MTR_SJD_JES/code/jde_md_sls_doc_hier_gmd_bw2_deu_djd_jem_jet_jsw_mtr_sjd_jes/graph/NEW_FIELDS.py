from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.config.ConfigStore import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CO_CD", col("SDKCOO"))\
        .withColumn(
          "PREV_DOC_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "PREV_DOC_LINE_NBR",
          lit(
            "#"
          )
        )\
        .withColumn("SUBSQ_DOC_NUM", col("SDDOCO"))\
        .withColumn("SUBSQ_DOC_LINE_NBR", col("SDLNID"))\
        .withColumn("SUBSQ_DOC_CAT_CD", col("SDDCTO"))\
        .withColumn(
          "PREV_DOC_TYPE_CD",
          lit(
            "#"
          )
        )\
        .withColumn(
          "PREV_DOC_CAT_CD",
          lit(
            "#"
          )
        )\
        .withColumn("BASE_UOM_CD", trim(col("SDUOM1")))\
        .withColumn("CRNCY_CD", trim(col("SDCRCD")))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                (lower(trim(col("SDTRDJ"))).isNull() | (trim(col("SDTRDJ")) == lit("")))
                | (trim(col("SDTRDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("SDTRDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDTRDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("MATL_NUM", trim(col("SDLITM")))\
        .withColumn(
          "CHG_DTTM",
          when(
              (
                (lower(trim(col("SDUPMJ"))).isNull() | (trim(col("SDUPMJ")) == lit("")))
                | (trim(col("SDUPMJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("SDUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("GRS_WT_MEAS", col("SDGRWT").cast(DecimalType(18, 4)))\
        .withColumn("WT_UOM_CD", trim(col("SDWTUM")))\
        .withColumn("VOL_MEAS", col("SDITVL").cast(DecimalType(18, 4)))\
        .withColumn("VOL_UOM_CD", trim(col("SDVLUM")))\
        .withColumn("SLS_UOM_CD", trim(col("SDUOM4")))\
        .withColumn("NET_WT_MEAS", col("SDITWT").cast(DecimalType(18, 4)))\
        .withColumn("MATL_MVMT_YR", substring((col("SDTRDJ") + lit(1900000)), 1, 4))\
        .withColumn(
          "SD_UNIQ_DOC_RL_ID",
          lit(
            "#"
          )
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CO_CD', CO_CD, 'PREV_DOC_NUM', PREV_DOC_NUM, 'PREV_DOC_LINE_NBR', PREV_DOC_LINE_NBR, 'SUBSQ_DOC_NUM', SUBSQ_DOC_NUM, 'SUBSQ_DOC_LINE_NBR', SUBSQ_DOC_LINE_NBR, 'SUBSQ_DOC_CAT_CD', SUBSQ_DOC_CAT_CD, 'PREV_DOC_TYPE_CD', PREV_DOC_TYPE_CD, 'PREV_DOC_CAT_CD', PREV_DOC_CAT_CD, 'SD_UNIQ_DOC_RL_ID', SD_UNIQ_DOC_RL_ID)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CO_CD', CO_CD, 'PREV_DOC_NUM', PREV_DOC_NUM, 'PREV_DOC_LINE_NBR', PREV_DOC_LINE_NBR, 'SUBSQ_DOC_NUM', SUBSQ_DOC_NUM, 'SUBSQ_DOC_LINE_NBR', SUBSQ_DOC_LINE_NBR, 'SUBSQ_DOC_CAT_CD', SUBSQ_DOC_CAT_CD, 'PREV_DOC_TYPE_CD', PREV_DOC_TYPE_CD, 'PREV_DOC_CAT_CD', PREV_DOC_CAT_CD, 'SD_UNIQ_DOC_RL_ID', SD_UNIQ_DOC_RL_ID)"
              )
            )
          )
        )\
        .withColumn("_deleted_", col("_deleted_"))
