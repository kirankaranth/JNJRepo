from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sls_doc_ptnr_func_hmd.config.ConfigStore import *
from sap_md_sls_doc_ptnr_func_hmd.udfs.UDFs import *

def SAP_VBPA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.VBPA")
