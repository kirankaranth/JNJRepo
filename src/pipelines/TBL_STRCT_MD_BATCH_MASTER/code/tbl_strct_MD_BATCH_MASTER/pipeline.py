from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_BATCH_MASTER.config.ConfigStore import *
from tbl_strct_MD_BATCH_MASTER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_BATCH_MASTER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BTCH = sql_MD_BTCH(spark)
    MD_BTCH(spark, df_sql_MD_BTCH)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_BATCH_MASTER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_BATCH_MASTER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
