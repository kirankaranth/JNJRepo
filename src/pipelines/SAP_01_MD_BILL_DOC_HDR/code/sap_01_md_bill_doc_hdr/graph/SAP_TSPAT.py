from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *

def SAP_TSPAT(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.TSPAT")
