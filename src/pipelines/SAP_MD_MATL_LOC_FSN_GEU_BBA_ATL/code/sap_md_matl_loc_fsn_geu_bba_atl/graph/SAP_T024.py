from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_fsn_geu_bba_atl.config.ConfigStore import *
from sap_md_matl_loc_fsn_geu_bba_atl.udfs.UDFs import *

def SAP_T024(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.T024")
