from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.config.ConfigStore import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.udfs.UDFs import *

def DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bba.knb1")
