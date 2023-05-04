from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_prch_delv_cnfrms_bbl_bbn.config.ConfigStore import *
from sap_md_prch_delv_cnfrms_bbl_bbn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_prch_delv_cnfrms_bbl_bbn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "91f5b56c-3d16-4d48-8826-8786f6013968", 
        "d4ac9cf3-c0a8-47fc-96f0-fe7d8a19eb59"
    )
    MD_PRCH_DELV_CNFRMS(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_DELV_CNFRMS_5")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_DELV_CNFRMS_5")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
