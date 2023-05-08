from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MVGR4"), col("BEZEI"))
