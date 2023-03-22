from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def MD_CUST_MSTR_UNLD_DATA(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_CUST_MSTR_UNLD_DATA")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_CUST_MSTR_UNLD_DATA")
