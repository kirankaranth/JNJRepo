from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.config.ConfigStore import *
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.udfs.UDFs import *

def SAP_TVAST(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tvast")
