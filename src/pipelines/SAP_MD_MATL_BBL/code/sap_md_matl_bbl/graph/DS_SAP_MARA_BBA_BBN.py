from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bbl.config.ConfigStore import *
from sap_md_matl_bbl.udfs.UDFs import *

def DS_SAP_MARA_BBA_BBN(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mara")
