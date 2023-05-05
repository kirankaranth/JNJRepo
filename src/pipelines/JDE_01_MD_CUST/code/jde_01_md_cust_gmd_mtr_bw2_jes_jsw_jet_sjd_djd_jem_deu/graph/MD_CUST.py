from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.config.ConfigStore import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.udfs.UDFs import *

def MD_CUST(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_CUST")
