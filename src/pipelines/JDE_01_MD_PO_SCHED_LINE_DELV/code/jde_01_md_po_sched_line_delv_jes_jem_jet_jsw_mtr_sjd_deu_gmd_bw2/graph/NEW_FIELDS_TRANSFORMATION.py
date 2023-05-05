from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.config.ConfigStore import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", col("pddoco"))\
        .withColumn("PO_LINE_NBR", col("pdlnid"))\
        .withColumn(
          "DELV_SCHED_CNT_NBR",
          lit(
            "#"
          )
        )\
        .withColumn("DELV_DTTM", lit(None))\
        .withColumn("SCHD_QTY", trim(col("pduorg")).cast(DecimalType(18, 4)))\
        .withColumn("RECV_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("STK_TFR_RECV_QTY", trim(col("pdurec")).cast(DecimalType(18, 4)))\
        .withColumn("MRP_ADJ_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn(
          "STAT_DELV_DTTM",
          when(
              (
                (lower(trim(col("pdopdj"))).isNull() | (trim(col("pddrqj")) == lit("")))
                | (trim(col("pddrqj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            concat(
              substring(trim(col("pddrqj")), 1, 4).cast(StringType()), 
              lit("-"), 
              substring(trim(col("pddrqj")), 5, 2).cast(StringType()), 
              lit("-"), 
              substring(trim(col("pddrqj")), 7, 2).cast(StringType())
            ), 
            "yyyy-MM-dd"
          ))
        )\
        .withColumn("CMT_QTY", lit(None))\
        .withColumn("ORDER_TYPE", col("pddcto"))\
        .withColumn("ORDER_CO", col("pdkcoo"))\
        .withColumn("ORDER_SUF", col("pdsfxo"))\
        .withColumn("PREV_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn(
          "CMT_DTTM",
          when(
              (
                (lower(trim(col("pdopdj"))).isNull() | (trim(col("pdopdj")) == lit("")))
                | (trim(col("pdopdj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            concat(
              substring(trim(col("pdopdj")), 1, 4).cast(StringType()), 
              lit("-"), 
              substring(trim(col("pdopdj")), 5, 2).cast(StringType()), 
              lit("-"), 
              substring(trim(col("pdopdj")), 7, 2).cast(StringType())
            ), 
            "yyyy-MM-dd"
          ))
        )\
        .withColumn("CAT_OF_DELV_DT", lit(None))\
        .withColumn("BTCH_NUM", lit(None))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR, 'ORDER_TYPE', ORDER_TYPE, 'ORDER_CO', ORDER_CO, 'ORDER_SUF', ORDER_SUF)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR, 'ORDER_TYPE', ORDER_TYPE, 'ORDER_CO', ORDER_CO, 'ORDER_SUF', ORDER_SUF)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))
