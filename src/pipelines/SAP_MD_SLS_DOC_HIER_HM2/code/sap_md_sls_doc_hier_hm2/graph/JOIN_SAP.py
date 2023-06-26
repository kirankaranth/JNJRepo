from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_hm2.udfs.UDFs import *

def JOIN_SAP(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in1.VBELV") == col("in0.`VBAK.VBELN`")), "inner")
