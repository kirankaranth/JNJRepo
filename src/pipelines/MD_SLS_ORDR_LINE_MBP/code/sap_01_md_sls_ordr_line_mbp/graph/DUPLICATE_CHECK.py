from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_mbp.config.ConfigStore import *
from sap_01_md_sls_ordr_line_mbp.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("COMPANY_CD"), 
        col("SLS_ORDR_DOC_ID"), 
        col("SLS_ORDR_LINE_NBR"), 
        col("SLS_ORDR_TYPE_CD")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
