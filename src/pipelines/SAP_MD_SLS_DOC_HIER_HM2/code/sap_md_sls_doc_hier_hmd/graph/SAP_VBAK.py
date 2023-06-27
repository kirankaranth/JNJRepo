from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hmd.config.ConfigStore import *
from sap_md_sls_doc_hier_hmd.udfs.UDFs import *

def SAP_VBAK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.vbak")
