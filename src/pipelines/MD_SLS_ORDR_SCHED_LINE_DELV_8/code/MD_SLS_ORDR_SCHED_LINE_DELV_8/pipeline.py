from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SLS_ORDR_SCHED_LINE_DELV_8.config.ConfigStore import *
from MD_SLS_ORDR_SCHED_LINE_DELV_8.udfs.UDFs import *
from prophecy.utils import *
from MD_SLS_ORDR_SCHED_LINE_DELV_8.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SLS_ORDR_SCHED_LINE_DELV = sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SLS_ORDR_SCHED_LINE_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "68d9e988-0ab7-44dd-b0f8-f40d63c03d3c", 
        "ea993d6b-b04b-439f-a8d5-f20617654ed1"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_8")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_8")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
