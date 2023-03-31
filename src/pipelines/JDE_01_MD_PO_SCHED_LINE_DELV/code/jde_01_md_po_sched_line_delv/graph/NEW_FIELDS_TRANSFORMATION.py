from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv.config.ConfigStore import *
from jde_01_md_po_sched_line_delv.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("sourceSystem", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", col("pddoco"))\
        .withColumn("PO_LINE_NBR", col("pdlnid"))\
        .withColumn(
          "DELV_SCHED_CNT_NBR",
          lit(
            "#"
          )
        )\
        .withColumn("DELV_DTTM", lit(None))\
        .withColumn("SCHD_QTY", trim(col("pduorg")))\
        .withColumn("RECV_QTY", lit(None))\
        .withColumn("STK_TFR_RECV_QTY", trim(col("pdurec")))\
        .withColumn("MRP_ADJ_QTY", lit(None))\
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
        .withColumn("PREV_QTY", lit(None))\
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
        .withColumn("DAI_CRT_DTTM", current_date())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt", col("_upt_"))\
        .withColumn(
          "_pk_   ",
          to_json(
            expr(
              "named_struct('sourceSystem', sourceSystem, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md_",
          md5(
            to_json(
              expr(
                "named_struct('sourceSystem', sourceSystem, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())
