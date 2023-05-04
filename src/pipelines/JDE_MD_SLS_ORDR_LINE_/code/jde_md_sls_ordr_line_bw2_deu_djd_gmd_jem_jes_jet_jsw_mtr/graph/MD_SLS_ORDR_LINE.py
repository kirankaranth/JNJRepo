from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.config.ConfigStore import *
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.udfs.UDFs import *

def MD_SLS_ORDR_LINE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_ORDR_LINE")
