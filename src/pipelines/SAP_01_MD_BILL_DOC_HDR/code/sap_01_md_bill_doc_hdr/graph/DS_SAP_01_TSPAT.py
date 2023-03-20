from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *

def DS_SAP_01_TSPAT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM hm2.tspat WHERE _deleted_ = 'F'")
