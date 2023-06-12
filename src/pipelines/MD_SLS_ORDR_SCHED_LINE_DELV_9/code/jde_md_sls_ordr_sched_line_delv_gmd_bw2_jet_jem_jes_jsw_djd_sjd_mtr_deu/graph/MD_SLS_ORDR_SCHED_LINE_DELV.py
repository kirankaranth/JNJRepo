from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu.config.ConfigStore import *
from jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu.udfs.UDFs import *

def MD_SLS_ORDR_SCHED_LINE_DELV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_ORDR_SCHED_LINE_DELV")
