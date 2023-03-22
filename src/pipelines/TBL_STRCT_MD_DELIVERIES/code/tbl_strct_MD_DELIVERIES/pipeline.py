from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_DELIVERIES.config.ConfigStore import *
from tbl_strct_MD_DELIVERIES.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_DELIVERIES.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SHIP_LINE = sql_MD_SHIP_LINE(spark)
    MD_SHIP_LINE(spark, df_sql_MD_SHIP_LINE)
    df_sql_MD_DELV_LINE = sql_MD_DELV_LINE(spark)
    df_sql_MD_DELV = sql_MD_DELV(spark)
    MD_DELV(spark, df_sql_MD_DELV)
    MD_DELV_LINE(spark, df_sql_MD_DELV_LINE)
    df_sql_MD_SHIP = sql_MD_SHIP(spark)
    MD_SHIP(spark, df_sql_MD_SHIP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_DELIVERIES")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_DELIVERIES")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
