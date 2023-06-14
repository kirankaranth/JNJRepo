from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_co_bba_bbn_bbl_hcs_mrs_atl_svs_fsn.config.ConfigStore import *
from sap_md_sup_co_bba_bbn_bbl_hcs_mrs_atl_svs_fsn.udfs.UDFs import *

def MD_SUP_CO(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SUP_CO")
