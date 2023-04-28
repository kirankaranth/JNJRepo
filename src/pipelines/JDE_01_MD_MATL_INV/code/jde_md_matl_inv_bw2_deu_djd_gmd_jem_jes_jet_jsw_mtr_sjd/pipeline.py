from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4101 = JDE_F4101(spark)
    df_JDE_F4101 = collectMetrics(
        spark, 
        df_JDE_F4101, 
        "graph", 
        "0Lhszmn7DhrOVbS6PTGma$$d5Kj6tlMD5fUdmVz8b70W", 
        "vQEAYCSsSfGZ8y2hiXHli$$ieCg1G8Y7sZ1-hkOXC2hc"
    )
    df_JDE_F41021 = JDE_F41021(spark)
    df_JDE_F41021 = collectMetrics(
        spark, 
        df_JDE_F41021, 
        "graph", 
        "l4XknEJa4nItR14JB7_y1$$cmLxEc1Wo1pK2xBRr2dzE", 
        "LCkx_vF1km9v1e2jHuJdU$$eivPtVkto7F6j3n8lTiw1"
    )
    df_DEL_FILTER = DEL_FILTER(spark, df_JDE_F41021)
    df_DEL_FILTER2 = DEL_FILTER2(spark, df_JDE_F4101)
    df_JOIN = JOIN(spark, df_DEL_FILTER, df_DEL_FILTER2)
    df_TRANSFORM = TRANSFORM(spark, df_JOIN)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_TRANSFORM)
    df_SET_FIELD_ORDER = SET_FIELD_ORDER(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER, 
        "graph", 
        "eocfCGB_Ntnf-mPShLDUL$$OSsJliJS0uB5U--nSZvGI", 
        "b46Ffqs-8HbCWzHvxvAmg$$W2KWWaiMsXop1XLVR4F8y"
    )
    MD_MATL_INV(spark, df_SET_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_MATL_INV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_MATL_INV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
