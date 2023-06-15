from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.config.ConfigStore import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("WADCTO"))\
        .withColumn("MFG_ORDR_NUM", col("WADOCO"))\
        .withColumn("LN_ITM_NBR", col("WALNID"))\
        .withColumn("PRDTN_UOM_CD", trim(col("WAUOM")))\
        .withColumn("BTCH_NUM", trim(col("WALOTN")))\
        .withColumn("DLV_CMPLT_IND", trim(col("WASRST")))\
        .withColumn("MATL_NUM", trim(col("WALITM")))\
        .withColumn("SCRP_QTY", col("WASOCN").cast(DecimalType(18, 4)))\
        .withColumn("ITM_QTY", col("WAUORG").cast(DecimalType(18, 4)))\
        .withColumn("PLNT_CD", trim(col("WAMCU")))\
        .withColumn("RCVD_QTY", col("WASOQS").cast(DecimalType(18, 4)))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM, 'LN_ITM_NBR', LN_ITM_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM, 'LN_ITM_NBR', LN_ITM_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
