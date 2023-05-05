from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_svs.config.ConfigStore import *
from sap_md_delv_svs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_svs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_sql_MD_DELV = collectMetrics(
        spark, 
        df_sql_MD_DELV, 
        "graph", 
        "5692c5fa-f2de-42f8-8d66-ac871f5c2567", 
        "41e75b66-fc5b-482e-83d3-ba21ba4b8d84"
    )
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "ee19a779-ad4c-437a-bb2d-9928bc065d03", 
        "1e2eb1ce-6d64-42e6-b4f4-5af319f7b86a"
    )
    MD_DELV(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_7")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_7")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
