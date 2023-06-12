from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_sls_ordr_sched_line_delv_bwi.config.ConfigStore import *
from md_sls_ordr_sched_line_delv_bwi.udfs.UDFs import *
from prophecy.utils import *
from md_sls_ordr_sched_line_delv_bwi.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SLS_ORDR_SCHED_LINE_DELV = sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SLS_ORDR_SCHED_LINE_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "816877ef-164c-48a9-b867-c398e72c9198", 
        "3b383557-c600-4634-96b5-7d58a5a364e6"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_6")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_6")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
