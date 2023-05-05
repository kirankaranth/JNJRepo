from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.config.ConfigStore import *
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "a69b2b21-9626-4f6f-874b-4985ac206711", 
        "4e95206d-213f-4567-90af-9846404fb355"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_9")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_9")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
