from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.config.ConfigStore import *
from md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.udfs.UDFs import *

def MD_CRNCY_TEXT(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_CRNCY_TEXT")
