from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_doc_ptnr_func.config.ConfigStore import *
from sap_01_md_sls_doc_ptnr_func.udfs.UDFs import *

def SAP_VBAK(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.VBAK WHERE _deleted_ = 'F'")
