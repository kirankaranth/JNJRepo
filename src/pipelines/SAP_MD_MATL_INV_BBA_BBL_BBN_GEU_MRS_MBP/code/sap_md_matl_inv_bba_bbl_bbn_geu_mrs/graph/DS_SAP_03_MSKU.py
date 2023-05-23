from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.udfs.UDFs import *

def DS_SAP_03_MSKU(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.msku")
