from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbn_geu_p01_mbp_svs.config.ConfigStore import *
from sap_01_md_sls_ordr_bbn_geu_p01_mbp_svs.udfs.UDFs import *

def SAP_T001(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.t001")
