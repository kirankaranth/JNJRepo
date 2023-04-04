from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PLANT.config.ConfigStore import *
from tbl_strct_MD_PLANT.udfs.UDFs import *

def MD_PLNT(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_PLNT")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PLNT")