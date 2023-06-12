from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_ldgr_jsw.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_jsw.udfs.UDFs import *

def MD_SLS_ORDR_HIST_LDGR(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"dev_md_l1.MD_SLS_ORDR_HIST_LDGR")
