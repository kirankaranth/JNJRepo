from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.config.ConfigStore import *
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.udfs.UDFs import *

def SAP_MARC(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.{Config.sourceTable} WHERE _deleted_ = 'F'")
