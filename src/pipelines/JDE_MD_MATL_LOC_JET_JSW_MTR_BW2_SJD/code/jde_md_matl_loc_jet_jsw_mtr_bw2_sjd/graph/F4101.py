from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.config.ConfigStore import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_sjd.udfs.UDFs import *

def F4101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable1}")
