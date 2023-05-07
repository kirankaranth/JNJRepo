from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_bbl.config.ConfigStore import *
from sap_01_md_sls_ordr_line_bbl.udfs.UDFs import *

def NEW_FIEDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_LINE_NBR', SLS_ORDR_LINE_NBR, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD, 'SRC_TBL_NM', SRC_TBL_NM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_LINE_NBR', SLS_ORDR_LINE_NBR, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD, 'SRC_TBL_NM', SRC_TBL_NM)"
              )
            )
          )
        )\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("RTE_FCTRY_CAL", trim(col("RTE_FCTRY_CAL")))\
        .withColumn("RTE_DESC", trim(col("RTE_DESC")))\
        .withColumn("DTRMN_PICK_PACK_TIME", trim(col("DTRMN_PICK_PACK_TIME")))\
        .withColumn("SHIPPING_PT", trim(col("SHIPPING_PT")))\
        .withColumn("CTRY_CD", trim(col("CTRY_CD")))\
        .withColumn("ITEM_CAT_CD", trim(col("ITEM_CAT_CD")))
