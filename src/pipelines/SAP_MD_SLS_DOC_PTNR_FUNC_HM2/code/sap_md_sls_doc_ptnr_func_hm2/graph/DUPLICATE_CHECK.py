from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sls_doc_ptnr_func_hm2.config.ConfigStore import *
from sap_md_sls_doc_ptnr_func_hm2.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("SLS_DOC_ITEM_NBR"), 
        col("SLS_DOC_ID"), 
        col("PTNR_FUNC_CD"), 
        col("COMPANY_CD"), 
        col("SLS_ORDR_TYPE_CD")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
