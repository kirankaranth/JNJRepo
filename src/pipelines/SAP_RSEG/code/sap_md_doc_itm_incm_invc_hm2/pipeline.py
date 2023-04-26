from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_doc_itm_incm_invc_hm2.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_doc_itm_incm_invc_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_RSEG = DS_SAP_RSEG(spark)
    df_DS_SAP_RSEG = collectMetrics(
        spark, 
        df_DS_SAP_RSEG, 
        "graph", 
        "BrkwQbdVP3hITOSUGxj6E$$Le3mC6S9Fy3sOnZFl7y-m", 
        "QYiFm0kQBqhkRc2pScpMC$$lUISyGTT10qzfcQeQGVe8"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_RSEG)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_NEW_FIELDS = collectMetrics(
        spark, 
        df_NEW_FIELDS, 
        "graph", 
        "klzwN9GulS6_TaNLHhz0v$$d2CznFeYIqYuRyqbXuCF3", 
        "G1pU8dyqCNoyiV9TY9Cp2$$7Mb7J7BMJGcUbho3xZ_Jq"
    )
    df_NEW_FIELDS.cache().count()
    df_NEW_FIELDS.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_RSEG")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_RSEG")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
