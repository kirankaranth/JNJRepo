from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.udfs.UDFs import *

def SAP_MARC(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.marc")
