from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.config.ConfigStore import *
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.udfs.UDFs import *

def MD_DELV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_DELV")
