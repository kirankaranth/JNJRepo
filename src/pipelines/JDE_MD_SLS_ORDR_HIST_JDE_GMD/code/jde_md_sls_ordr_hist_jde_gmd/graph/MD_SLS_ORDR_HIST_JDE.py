from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_jde_gmd.config.ConfigStore import *
from jde_md_sls_ordr_hist_jde_gmd.udfs.UDFs import *

def MD_SLS_ORDR_HIST_JDE(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable(f"dev_md_l1.md_sls_ordr_hist_jde")
