from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sls_ordr_sched_line_delv_10_hmd.config.ConfigStore import *
from sap_md_sls_ordr_sched_line_delv_10_hmd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_ordr_sched_line_delv_10_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SLS_ORDR_SCHED_LINE_DELV = sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SLS_ORDR_SCHED_LINE_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "6b85a1f4-57bf-4dfb-b600-a1c8a411fe41", 
        "2dabe353-e2c1-4910-8d0b-f8ead9828649"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_10")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SLS_ORDR_SCHED_LINE_DELV_10")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
