from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MANDT_04 = MANDT_04(spark)
    df_NEW_FIELDS_01 = NEW_FIELDS_01(spark, df_MANDT_04)
    df_SET_FIELD_ORDER = SET_FIELD_ORDER(spark, df_NEW_FIELDS_01)
    df_SET_FIELD_ORDER = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER, 
        "graph", 
        "RiGaKImVXmrTgpG8J5XQt$$uk3dlYvYcouECl_4mV3-H", 
        "DFaLa2MoPFtV52vBhFK8p$$8QGAwvSBHc53V1LCgjuA7"
    )
    MD_MATL_UOM(spark, df_SET_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_UOM_JET_JES_JEM_JSW_BW2_DJD_SJD_DEU_GMD_MTR")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_MD_MATL_UOM_JET_JES_JEM_JSW_BW2_DJD_SJD_DEU_GMD_MTR"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
