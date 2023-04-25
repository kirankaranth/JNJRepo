from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *

def MD_PO_SCHED_LINE_DELV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PO_SCHED_LINE_DELV")
