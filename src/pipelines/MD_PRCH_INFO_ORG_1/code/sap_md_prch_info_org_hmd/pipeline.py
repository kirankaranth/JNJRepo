from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_prch_info_org_hmd.config.ConfigStore import *
from sap_md_prch_info_org_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_prch_info_org_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_INFO_ORG = sql_MD_PRCH_INFO_ORG(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_INFO_ORG)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "063b4d10-1697-45dd-9f5c-d023c10813d8", 
        "80a302b0-357b-4eea-81ff-6a28972d6e62"
    )
    MD_PRCH_INFO_ORG(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_INFO_ORG_1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_INFO_ORG_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
