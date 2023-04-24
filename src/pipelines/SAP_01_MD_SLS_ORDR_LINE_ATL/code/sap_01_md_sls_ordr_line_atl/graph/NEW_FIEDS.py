from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_atl.config.ConfigStore import *
from sap_01_md_sls_ordr_line_atl.udfs.UDFs import *

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
        .withColumn("_deleted_", lit("F"))
