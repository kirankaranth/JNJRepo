from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.udfs.UDFs import *

def DS_SAP_03_VBUP(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.vbup")
