from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_prch_delv_cnfrms_svs.config.ConfigStore import *
from sap_md_prch_delv_cnfrms_svs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_prch_delv_cnfrms_svs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "8e7ae566-aef9-4f94-8ec9-b13c6099c88f", 
        "bcc1d5a2-7782-4af2-9106-89fd0562d0c6"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_PRCH_DELV_CNFRMS_SVS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_PRCH_DELV_CNFRMS_SVS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
