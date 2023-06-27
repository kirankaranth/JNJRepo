from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hmd.config.ConfigStore import *
from sap_md_sls_doc_hier_hmd.udfs.UDFs import *

def FIELDS_VBAK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("BUKRS_VF"), 
        col("AUART"), 
        col("VBELN").alias("VBAK.VBELN"), 
        col("_deleted_").alias("VBAK._deleted_"), 
        col("_upt_").alias("VBAK._upt_")
    )
