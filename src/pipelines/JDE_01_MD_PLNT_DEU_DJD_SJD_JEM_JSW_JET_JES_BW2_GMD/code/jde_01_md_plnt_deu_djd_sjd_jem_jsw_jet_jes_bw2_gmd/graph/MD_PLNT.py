from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *

def MD_PLNT(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PLNT")
