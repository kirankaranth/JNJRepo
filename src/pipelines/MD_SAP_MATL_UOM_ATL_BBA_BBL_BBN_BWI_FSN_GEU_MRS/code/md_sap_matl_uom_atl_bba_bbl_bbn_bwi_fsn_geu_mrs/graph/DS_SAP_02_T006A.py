from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_sap_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs.config.ConfigStore import *
from md_sap_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs.udfs.UDFs import *

def DS_SAP_02_T006A(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.DBTABLE2}")
