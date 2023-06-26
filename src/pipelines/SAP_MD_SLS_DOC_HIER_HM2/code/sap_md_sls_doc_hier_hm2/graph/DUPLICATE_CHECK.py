from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_hm2.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("CO_CD"), 
        col("PREV_DOC_NUM"), 
        col("PREV_DOC_LINE_NBR"), 
        col("SUBSQ_DOC_NUM"), 
        col("SUBSQ_DOC_LINE_NBR"), 
        col("SUBSQ_DOC_CAT_CD"), 
        col("PREV_DOC_TYPE_CD"), 
        col("PREV_DOC_CAT_CD"), 
        col("SD_UNIQ_DOC_RL_ID")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
