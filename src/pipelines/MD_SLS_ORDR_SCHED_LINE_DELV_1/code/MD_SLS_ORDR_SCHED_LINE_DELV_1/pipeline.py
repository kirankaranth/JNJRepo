from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SLS_ORDR_SCHED_LINE_DELV_1.config.ConfigStore import *
from MD_SLS_ORDR_SCHED_LINE_DELV_1.udfs.UDFs import *
from prophecy.utils import *
from MD_SLS_ORDR_SCHED_LINE_DELV_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SLS_ORDR_SCHED_LINE_DELV = sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SLS_ORDR_SCHED_LINE_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "a5f882bd-ce16-4a19-ad57-cfe43a8433fb", 
        "cb68d93e-f11e-4bcb-b25b-4a76cf0c08f3"
    )
    MD_SLS_ORDR_SCHED_LINE_DELV(spark, df_addL1fields)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
