from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_mbp_fsn_atl.config.ConfigStore import *
from sap_md_co_cd_mbp_fsn_atl.udfs.UDFs import *

def DS_SAP_HMD_KNB1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.knb1")
