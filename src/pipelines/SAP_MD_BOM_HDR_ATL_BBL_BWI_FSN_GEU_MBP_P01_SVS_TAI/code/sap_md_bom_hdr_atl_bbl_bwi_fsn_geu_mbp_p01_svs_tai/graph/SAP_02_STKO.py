from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.config.ConfigStore import *
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.udfs.UDFs import *

def SAP_02_STKO(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.stko WHERE _deleted_ = 'F'")