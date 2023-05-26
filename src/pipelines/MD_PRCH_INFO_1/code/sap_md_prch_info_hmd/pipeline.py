from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_prch_info_hmd.config.ConfigStore import *
from sap_md_prch_info_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_prch_info_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_INFO = sql_MD_PRCH_INFO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_INFO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "902e870a-1b1e-4b08-a8ed-bad8baac7e50", 
        "5d4ba3e3-af55-4e4c-a4b2-53658149e859"
    )
    MD_PRCH_INFO(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_INFO_1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_INFO_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
