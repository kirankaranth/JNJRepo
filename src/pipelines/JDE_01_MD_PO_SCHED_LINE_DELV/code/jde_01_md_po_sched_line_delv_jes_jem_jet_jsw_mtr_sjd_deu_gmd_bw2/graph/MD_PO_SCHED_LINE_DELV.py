from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.config.ConfigStore import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.udfs.UDFs import *

def MD_PO_SCHED_LINE_DELV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PO_SCHED_LINE_DELV")
