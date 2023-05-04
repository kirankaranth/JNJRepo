from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bill_doc_hdr_bba_bbn.config.ConfigStore import *
from sap_md_bill_doc_hdr_bba_bbn.udfs.UDFs import *

def DS_SAP_01_TVKOT(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tvkot")
