from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_sched_line_delv_01.config.ConfigStore import *
from sap_md_po_sched_line_delv_01.udfs.UDFs import *

def SAP_EKET(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
