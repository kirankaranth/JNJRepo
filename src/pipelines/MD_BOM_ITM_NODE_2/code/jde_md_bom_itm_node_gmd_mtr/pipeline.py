from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_bom_itm_node_gmd_mtr.config.ConfigStore import *
from jde_md_bom_itm_node_gmd_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_md_bom_itm_node_gmd_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BOM_ITM_NODE = sql_MD_BOM_ITM_NODE(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_BOM_ITM_NODE)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "8f238a2b-86d7-409a-9e93-35c0515ede6f", 
        "59daf43e-2577-4c54-8fc8-5a7c6bda484b"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_BOM_ITM_NODE_2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_BOM_ITM_NODE_2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
