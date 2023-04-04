from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def MD_SUP_PRCHSNG_ORG(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable(f"dev_md_l1.md_sup_prchsng_org")
