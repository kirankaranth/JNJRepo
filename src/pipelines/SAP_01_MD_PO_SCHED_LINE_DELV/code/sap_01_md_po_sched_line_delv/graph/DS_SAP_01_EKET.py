from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_po_sched_line_delv.config.ConfigStore import *
from sap_01_md_po_sched_line_delv.udfs.UDFs import *

def DS_SAP_01_EKET(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bwi.eket WHERE _deleted_ = 'F'")
