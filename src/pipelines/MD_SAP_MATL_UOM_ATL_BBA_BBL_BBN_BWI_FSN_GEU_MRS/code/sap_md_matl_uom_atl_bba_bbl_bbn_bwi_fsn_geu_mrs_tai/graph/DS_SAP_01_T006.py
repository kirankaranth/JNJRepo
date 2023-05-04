from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.config.ConfigStore import *
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.udfs.UDFs import *

def DS_SAP_01_T006(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.DBTABLE1}")
