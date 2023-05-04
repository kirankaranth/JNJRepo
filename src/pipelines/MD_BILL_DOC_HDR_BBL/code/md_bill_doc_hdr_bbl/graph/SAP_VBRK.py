from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *

def SAP_VBRK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.vbrk")
