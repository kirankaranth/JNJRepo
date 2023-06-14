from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_bom_itm_node_sjd.config.ConfigStore import *
from jde_md_bom_itm_node_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_bom_itm_node_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BOM_ITM_NODE = sql_MD_BOM_ITM_NODE(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_BOM_ITM_NODE)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "75ac8a96-fae3-4d07-9a08-85e90859ab70", 
        "cc1acc8f-a1ba-4f05-ac55-44b3161159d1"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_BOM_ITM_NODE_4")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_BOM_ITM_NODE_4")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
