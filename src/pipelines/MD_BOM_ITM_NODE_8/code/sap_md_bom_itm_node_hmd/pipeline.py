from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_bom_itm_node_hmd.config.ConfigStore import *
from sap_md_bom_itm_node_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_bom_itm_node_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BOM_ITM_NODE = sql_MD_BOM_ITM_NODE(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_BOM_ITM_NODE)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "bd3c32d4-e9d2-45b6-88a9-9e1fabacc39c", 
        "bea733e5-addc-4780-836c-af026c78cd53"
    )
    MD_BOM_ITM_NODE(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_BOM_ITM_NODE_8")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_BOM_ITM_NODE_8")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
